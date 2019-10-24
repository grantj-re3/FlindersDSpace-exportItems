#!/usr/bin/ruby
#--
# Copyright (c) 2019, Flinders University, South Australia. All rights reserved.
# Contributors: Library, Corporate Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#
# PURPOSE
#
# The purpose of this program is to export DSpace 5.x item metadata including
# embargo info and pointers to fulltext/bitstreams into a file suitable for
# loading into another system.
#
# ALGORITHM
#
# Extract rich XML metadata via DSpace (AIP) packager tool and enhance it
# by extracting additional information from the DSpace database. For each
# item_id (eg. from a DSpace Batch Metadata Editing Tool CSV file):
#
# - Extract DB info including handle using item_id
# - Extract XML info via DSpace (AIP) packager tool using handle
# - Create a resulting XML file which includes:
#   * DB info in XPath /dspace_item/custom
#   * package info in XPath /dspace_item/mets
# - Create CSV file which includes info for review
#
#++
##############################################################################

# Add dirs to the library path
$: << File.expand_path("../lib", File.dirname(__FILE__))
$: << File.expand_path("../lib/libext", File.dirname(__FILE__))
$: << File.expand_path(".", File.dirname(__FILE__))
$: << "#{ENV['HOME']}/.ds/etc"

require 'date'
require "rexml/document"
require 'rexml/xpath'
require 'fileutils'
require 'pp'
require 'faster_csv'

require 'rubygems'
require 'pg'

require 'export_items_config'
require 'object_extra'
require 'pg_extra'
require 'dbc'
require 'dspace_utils'
require 'dspace_pg_utils'

##############################################################################
# Extend class REXML::Attributes
##############################################################################
module REXML
  class Attributes
    alias _each_attribute each_attribute

    # Sort attributes by name
    def each_attribute(&blk)
      to_enum(:_each_attribute).sort_by{|attr| attr.name}.each(&blk)
    end
  end
end

##############################################################################
# A class for exporting DSpace item metadata
##############################################################################
class Item2Export
  include DSpacePgUtils
  include Item2ExportConfig

  EMBARGO_REF_DATE = Date.today

  DUMMY_ID = -1			# Used to replace a NULL database ID
  S_OK_ACTION_IDS = [:read, :withdrawn_read].map{|action| POLICY_ACTION_IDS[action].to_s}
  S_OK_ACTION_IDS << DUMMY_ID.to_s

  HOW_TO_MATCH_LIST = [
    :DspaceDoiToPureRmid,
    :DspaceRmidToPureRmid,
  ]

  # {:DspaceDoiToPureRmid => :doi,  DspaceRmidToPureRmid => :rmid}
  CLEAN_ID_TYPES = HOW_TO_MATCH_LIST.inject({}){|h,how|
    h[how] = how.to_s.gsub(/^Dspace(.+)ToPure.*$/, "\\1").downcase.to_sym
    h
  }

  @@how_to_match = nil		# An element of HOW_TO_MATCH_LIST

  # @@rmids_by_doi[doi1] = [rmid1a, rmid1b, ...]
  @@rmids_by_doi = {}

  # @@rmids[rmid1] = true
  @@rmids = {}

  @@is_open_access = nil

  ############################################################################
  def initialize(item_id)
    @item_id = item_id			# DSpace item_id
    @item = nil				# Item info from DB
    @attrs = {}				# Attributes derived from item info above
    @doc = nil				# XML document

    @dc = nil				# Some dublin core values
    @misc = nil				# Other derived item info
  end

  ############################################################################
  def bitstream_text_value_clause(element_name)
    # For element_name 'title', clause will extract bitstream filename
    # For element_name 'description', clause will extract bitstream file description
    <<-SQL_BITSTREAM_TEXT_VALUE_CLAUSE.gsub(/^\t*/, '')
	      coalesce((select text_value from metadatavalue where resource_type_id=0 and resource_id=b.bitstream_id and metadata_field_id in
	        (select metadata_field_id from metadatafieldregistry where qualifier is null and element='#{element_name}')), '')
    SQL_BITSTREAM_TEXT_VALUE_CLAUSE
  end

  ############################################################################
  def get_item_sql_query
    # FIXME: Use SQL COALESCE for start_date, etc.
    bundle_title = 'ORIGINAL'
    bundle_clause = <<-SQL_BUNDLE_CLAUSE.gsub(/^\t*/, '')
	            (select bundle_id from item2bundle i2b where i2b.item_id=#{@item_id}) and metadata_field_id =
	            (select metadata_field_id from metadatafieldregistry where element='title' and qualifier is null and metadata_schema_id=
	              (select metadata_schema_id from metadataschemaregistry where short_id='dc')
	            )
    SQL_BUNDLE_CLAUSE

    sql = <<-SQL_GET_ITEM.gsub(/^\t*/, '')
	select
	  (select handle from handle h where h.resource_type_id=#{RESOURCE_TYPE_IDS[:item]} and h.resource_id=i.item_id) item_hdl,
	  item_id, in_archive, withdrawn, discoverable,

	  array_to_string(array(
	    select i2.item_id || '^' || coalesce(policy_id,#{DUMMY_ID}) || '^' || coalesce(action_id,#{DUMMY_ID}) || '^' ||
              coalesce(to_char(p.start_date, 'YYYY-MM-DD'), '')
	    from item i2 left join resourcepolicy p
            on (p.epersongroup_id=#{EPERSON_GROUP_IDS[:public]} and p.resource_type_id=#{RESOURCE_TYPE_IDS[:item]} and p.resource_id=i2.item_id)

            where i2.item_id=#{@item_id}
	  ), '||') item_policies,

	  (select mdv.resource_id || '^' || coalesce(p.policy_id,#{DUMMY_ID}) || '^' || coalesce(p.action_id,#{DUMMY_ID}) || '^' ||
             coalesce(to_char(p.start_date, 'YYYY-MM-DD'), '') || '^#{bundle_title}'
	   from metadatavalue mdv left join resourcepolicy p
	   on (p.epersongroup_id=#{EPERSON_GROUP_IDS[:public]} and p.resource_type_id=#{RESOURCE_TYPE_IDS[:bundle]} and p.resource_id=mdv.resource_id)

	   where mdv.text_value='ORIGINAL' and mdv.resource_type_id=#{RESOURCE_TYPE_IDS[:bundle]} and mdv.resource_id in
             #{bundle_clause}
	  ) bundle_policy,

	  array_to_string(array(
	    select b.bitstream_id || '^' || coalesce(policy_id,#{DUMMY_ID}) || '^' || coalesce(p.action_id,#{DUMMY_ID}) || '^' ||
              coalesce(to_char(p.start_date, 'YYYY-MM-DD'), '') || '^' ||
	      deleted || '^' || sequence_id || '^' || size_bytes || '^' || internal_id || '^' ||
	      #{bitstream_text_value_clause('title')} || '^' ||
	      #{bitstream_text_value_clause('description')} || '^' ||
	      (select mimetype from bitstreamformatregistry where bitstream_format_id=b.bitstream_format_id)
	    from bitstream b left join resourcepolicy p
	    on (p.epersongroup_id=#{EPERSON_GROUP_IDS[:public]} and p.resource_type_id=#{RESOURCE_TYPE_IDS[:bitstream]} and p.resource_id=b.bitstream_id)

	    where b.deleted='f' and b.bitstream_id in
	      (select bitstream_id from bundle2bitstream where bundle_id=
	        (select resource_id from metadatavalue where text_value='#{bundle_title}' and resource_type_id=#{RESOURCE_TYPE_IDS[:bundle]} and resource_id in
                  #{bundle_clause}
	        )
	      )
	  ), '||') bitstream_policies
	from item i
	where i.item_id=#{@item_id} ;
    SQL_GET_ITEM
    sql
  end

  ############################################################################
  def get_item_from_db
    sql = get_item_sql_query
    @item = {}
    PG::Connection.connect2(DB_CONNECT_INFO){|conn|
      conn.exec(sql){|result|
        length = result.inject(0){|sum,o| sum+=1}
        unless length == 1
          STDERR.puts "ERROR: Expected 1 but got #{length} rows for SQL query:"
          STDERR.puts "#{sql}"
          exit(1)
        end

        result.each{|row|
          @item = {
            :item_id			=> row['item_id'],
            :handle			=> row['item_hdl'],
            :url			=> "#{HANDLE_URL_LEFT_STRING}#{row['item_hdl']}",

            :in_archive			=> row['in_archive'],
            :withdrawn			=> row['withdrawn'],
            :discoverable		=> row['discoverable'],

            :item_policies		=> row['item_policies'],
            :bundle_policy		=> row['bundle_policy'],
            :bitstream_policies		=> row['bitstream_policies'],
          } 

        }
      }
    }
    STDERR.puts "@item: #{@item.pretty_inspect}" if DEBUG
  end

  ############################################################################
  def debug_db_info_attrs
    @item.inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attr-key is a string
  end

  ############################################################################
  def item_ids_attrs
    attrs = {}					# XML attributes
    [
      :item_id,
      :handle,
      :url,
    ].each{|k| attrs[k] = @item[k]}
    attrs.inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attr-key is a string
  end

  ############################################################################
  def item_status_attrs
    attrs = {}					# XML attributes
    [
      :in_archive,
      :withdrawn,
      :discoverable,
    ].each{|k| attrs[k] = @item[k]}
    attrs.inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attr-key is a string
  end

  ############################################################################
  def slice_into_item_embargo_attrs(s_item_policy)
    sf = {}					# DB subfields
    sf[:item_id], sf[:policy_id], sf[:action_id], sf[:start_date] = s_item_policy.split(SUBFIELD_DELIM)
    # FIXME:
    # - Deal with policy action 12 (WITHDRAWN_READ) [item.withdrawn='t'] later.
    # - Consider adding <item_status state="ignore" reason="withdrawn"/> vs <item_status state="ok"
    # - Consider writing to different output dir?
    unless S_OK_ACTION_IDS.include?(sf[:action_id])
      STDERR.puts "ERROR: For item_id #{@item_id}, expected action_id of #{S_OK_ACTION_IDS.join(',')} but got '#{sf[:action_id]}'."
      exit(2)
    end

    attrs = {}					# XML attributes
    [
      :policy_id,
      :action_id,
    ].each{|k| attrs[k] = sf[k]}

    if sf[:start_date] && !sf[:start_date].empty?
      o_start_date = Date.parse(sf[:start_date])
      STDERR.printf "%-25s: start_date: %s\n", __method__, o_start_date.strftime(DEBUG_DATE_FMT) if DEBUG

      if o_start_date > EMBARGO_REF_DATE
        attrs[:has_embargo] = "true"
        attrs[:lift_date] = sf[:start_date]

      else
        attrs[:has_embargo] = "false"
      end

    else
      attrs[:has_embargo] = "false"
    end
    attrs
  end

  ############################################################################
  def item_embargo_attrs(h_item_attrs)
    return nil unless h_item_attrs
    h_item_attrs.inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attrs-key is a string
  end

  ###########################################################################
  def slice_into_bundle_embargo_attrs
    return nil unless @item[:bundle_policy]

    sf = {}					# Subfields
    sf[:bundle_id], sf[:policy_id], sf[:action_id], sf[:start_date], sf[:bundle_title] = @item[:bundle_policy].split(SUBFIELD_DELIM)
    # FIXME:
    # - Deal with policy action 12 (WITHDRAWN_READ) [item.withdrawn='t'] later.
    # - Consider adding <item_status state="ignore" reason="withdrawn"/> vs <item_status state="ok"
    # - Consider writing to different output dir?
    unless S_OK_ACTION_IDS.include?(sf[:action_id])
      STDERR.puts "ERROR: For bundle_id #{sf[:bundle_id]}, expected action_id of #{S_OK_ACTION_IDS.join(',')} but got '#{sf[:action_id]}'."
      exit(3)
    end

    attrs = {}					# XML attributes
    [
      :bundle_id,
      :policy_id,
      :action_id,
      :bundle_title,
    ].each{|k| attrs[k] = sf[k]}

    if sf[:start_date] && !sf[:start_date].empty?
      o_start_date = Date.parse(sf[:start_date])
      STDERR.printf "%-25s: start_date: %s\n", __method__, o_start_date.strftime(DEBUG_DATE_FMT) if DEBUG

      if o_start_date > EMBARGO_REF_DATE
        attrs[:has_embargo] = "true"
        attrs[:lift_date] = sf[:start_date]

      else
        attrs[:has_embargo] = "false"
      end

    else
      attrs[:has_embargo] = "false"
    end
    attrs
  end

  ###########################################################################
  def bundle_embargo_attrs
    return nil unless @attrs[:bundle]
    @attrs[:bundle].inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attrs-key is a string
  end

  ###########################################################################
  def slice_into_bitstream_embargo_attrs(s_bitstream_policy)
    sf = {}					# Subfields
    sf[:bitstream_id], sf[:policy_id], sf[:action_id], sf[:start_date],
      sf[:deleted], sf[:seq], sf[:bytes], sf[:internal_id],
      sf[:fname], sf[:fdesc], sf[:fmime] = s_bitstream_policy.split(SUBFIELD_DELIM)
    # FIXME:
    # - Deal with policy action 12 (WITHDRAWN_READ) [item.withdrawn='t'] later.
    # - Consider adding <item_status state="ignore" reason="withdrawn"/> vs <item_status state="ok"
    # - Consider writing to different output dir?
    unless S_OK_ACTION_IDS.include?(sf[:action_id])
      STDERR.puts "ERROR: For bitstream_id #{sf[:bitstream_id]}, expected action_id of #{S_OK_ACTION_IDS.join(',')} but got '#{sf[:action_id]}'."
      exit(4)
    end


    # FIXME: Process: :deleted
    rel_fpath = sf[:internal_id].sub(/^((\d\d)(\d\d)(\d\d)(.*))$/, '\2/\3/\4/\1')
    attrs = {
      :fpath     => DSPACE_ASSETSTORE_DIRPATH  + rel_fpath,
      :fpath_url => DSPACE_ASSETSTORE_BASE_URL + rel_fpath,
    }					# XML attributes
    (sf.keys - [:start_date, :internal_id]).each{|k| attrs[k] = sf[k]}

    attrs[:docversion] = case sf[:fdesc].to_s
    when /author/i
      "author"
    when /publish/i
      "publisher"
    else
      "unknown"
    end

    if sf[:start_date] && !sf[:start_date].empty?
      o_start_date = Date.parse(sf[:start_date])
      STDERR.printf "%-25s: start_date: %s\n", __method__, o_start_date.strftime(DEBUG_DATE_FMT) if DEBUG

      if o_start_date > EMBARGO_REF_DATE
        attrs[:has_embargo] = "true"
        attrs[:lift_date] = sf[:start_date]

      else
        attrs[:has_embargo] = "false"
      end

    else
      attrs[:has_embargo] = "false"
    end
    attrs[:file_title] = attrs[:fname]
    attrs[:is_open_access] = @@is_open_access.inspect
    attrs
  end

  ###########################################################################
  def bitstream_embargo_attrs(h_bitstream_attrs)
    h_bitstream_attrs.inject({}){|h,(k,v)|
      h[k.to_s] = k==:docversion ? VERBOSE_DOCVERSIONS[v] : v
      h
    }	# Ensure attrs-key is a string
  end

  ############################################################################
  def slice_into_attrs
    # Items
    @attrs[:item] = []
    if @item[:item_policies].to_s.empty?
      @attrs[:item] << nil		# Withdrawn item

    else
      @item[:item_policies].split(MULTIVALUE_DELIM).each{|ip|
        @attrs[:item] << slice_into_item_embargo_attrs(ip)
      }
    end

    # Bundle
    @attrs[:bundle] = slice_into_bundle_embargo_attrs

    # Bitstreams
    @attrs[:bitstream] = nil
    if @attrs[:bundle]
      @attrs[:bitstream] = []
      @item[:bitstream_policies].split(MULTIVALUE_DELIM).each{|bsp|
        @attrs[:bitstream] << slice_into_bitstream_embargo_attrs(bsp)
      }
    end
  end

  ############################################################################
  def create_custom_xml
    @doc = REXML::Document.new "<dspace_item><custom/></dspace_item>"

    e = REXML::XPath.first(@doc, "//custom")
    e.add_element("debug_db_info", debug_db_info_attrs)	# Database debug info
    e.add_element("item_ids", item_ids_attrs)
    e.add_element("item_status", item_status_attrs)

    STDERR.printf "\nEMBARGO_REF_DATE                     : %s\n", EMBARGO_REF_DATE.strftime(DEBUG_DATE_FMT) if DEBUG
    @attrs[:item].each{|ia|
      e.add_element("item_embargo", item_embargo_attrs(ia))
    }

    if bundle_embargo_attrs
      e.add_element("bundle_embargo", bundle_embargo_attrs)

      @attrs[:bitstream].each{|ba|
        e.add_element("bitstream_embargo", bitstream_embargo_attrs(ba))
      }
    end
    add_match_info_to_xml(e)

    STDERR.puts "Get & add DSpace XML package to custom XML" if DEBUG
    add_item_package_to_xml
  end

  ############################################################################
  def will_omit_item
    if ITEM_STATUS_OK.keys.any?{|k| @item[k].nil?}
      STDERR.puts "ERROR: One or more attributes #{ITEM_STATUS_OK.keys.inspect} are nil in #{@item.inspect}"
      exit(11)
    end

    return true if ItemIdsOmit.item_list.include?(@item_id)	# Omit
    return true if ITEM_STATUS_OK.any?{|k,v| @item[k]!=v}	# Omit
    false							# Keep
  end

  ############################################################################
  def save_custom_xml
    fname_out = calc_pathname(will_omit_item ? :out_omit : :out)
    FileUtils.mkdir_p(File.dirname(fname_out))

    File.open(fname_out, "w"){|f|
      @doc.write(f, 2)
      f.puts
    }
  end

  ############################################################################
  def calc_ids_pathname(rmid)
    dir = rmid[0..5]
    fname = "#{FILE_PREFIX_PURE_RSOUT}#{rmid}.#{FILE_EXT_PURE_RSOUT}"
    "#{DIR_PURE_RSOUT}/#{dir}/#{fname}"
  end

  ############################################################################
  def calc_pathname(type)
    unless @item && @item[:handle]
      STDERR.puts "ERROR: Unable to determine file path for '#{type}'. Handle not found."
      exit(5)
    end

    fpart = @item[:handle].sub(/\//, "_")	# "111/2222" --> "111_2222"
    case type
      when :pkg
        "#{DIR_DSPACE_PACKAGE}/#{BASENAME_DSPACE_PACKAGE}#{fpart}.xml"

      when :out
        "#{DIR_DSPACE_OUT}/#{fpart}.d/#{BASENAME_DSPACE_OUT}#{fpart}.xml"

      when :out_omit
        "#{DIR_DSPACE_OUT_OMIT}/#{fpart}.d/#{BASENAME_DSPACE_OUT}#{fpart}.xml"

      else
        STDERR.puts "ERROR: Invalid file path type '#{type}'."
        exit(6)
    end
  end

  ############################################################################
  def get_item_package
    # If we already have a readable item package AND we are not forced
    # to get it again, then bypass this method (thus saving ~12 seconds
    # per item on some systems).
    return if File.readable?(calc_pathname(:pkg)) && !FORCE_GET_ITEM_PKG

    fpath = calc_pathname(:pkg)
    item_hdl = @item[:handle]

    # Automatically makes parent dirs as required
    cmd = "#{DSPACE_PKG_CMD_PART1} --identifier #{item_hdl} #{fpath}"
    STDERR.puts "Command: #{cmd}"
    output = %x{ #{cmd} }		# Execute OS command
    res = $?

    unless res.to_s == "0"
      puts "ERROR: Return code #{res} when executing command:\n  #{cmd}"
      exit(7)
    end
  end

  ############################################################################
  def add_item_package_to_xml
    unless File.readable?(calc_pathname(:pkg))
      puts "ERROR: Cannot find DSpace package file: #{calc_pathname(:pkg)}"
      exit(8)
    end

    File.open(calc_pathname(:pkg)){|f|
      doc_pkg = REXML::Document.new(f)
      e_pkg = doc_pkg.root	# Point to DSpace package XML
      @doc.root << e_pkg	# Add package tree to our custom XML doc
    }
  end

  ############################################################################
  def get_pure_rec_ids(rmid)
    pathname = calc_ids_pathname(rmid)
    unless File.readable?(pathname)
      STDERR.puts "ERROR: Cannot find XML file: #{pathname}"
      exit(8)
    end

    ids = {}		# Key-value pairs (where key is unique)
    other_ids = []	# Name-value pairs (where name may be repeated)
    rec_name = nil	# Name of the root element of the record
    File.open(pathname){|f|
      doc = REXML::Document.new(f)
      rec_name = doc.root.name
      ids[:pureId]		= doc.root.attributes["pureId"]
      ids[:uuid]		= doc.root.attributes["uuid"]

      # Potentially rmid != externalId (since rmid may match an additionalExternalId)
      ids[:externalId]		= doc.root.attributes["externalId"]

      xpath_top = "/#{@misc[:pure_rec_name]}"
      xpath = "#{xpath_top}/#{XPATH_RSOUT_DOI}"
      doc.elements.each(xpath){|e| other_ids << [:doi, e.text]}

      xpath = "#{xpath_top}/#{XPATH_RSOUT_EXT_ID}"
      doc.elements.each(xpath){|e| other_ids << [e.attributes["idSource"].to_sym, e.text]}

      xpath = "#{xpath_top}/#{XPATH_RSOUT_UUID}"
      doc.elements.each(xpath){|e| other_ids << [:previousUuid, e.text]}

      xpath = "#{xpath_top}/#{XPATH_RSOUT_PORTAL}"
      doc.elements.each(xpath){|e| other_ids << [:portalUrl, e.text]}
    }
    @misc[:by_rmid][rmid][:pure_rec_name] = rec_name
    @misc[:by_rmid][rmid][:pure_rec_ids] = ids
    @misc[:by_rmid][rmid][:pure_rec_other_ids] = other_ids
  end

  ############################################################################
  def add_match_info_to_xml(elem)
    by_rmid = @misc[:by_rmid]
    matches_attrs = {
      "type"			=> @@how_to_match.to_s,
      "num_matching_ids"	=> by_rmid.size,
      "is_unique"		=> (by_rmid.size == 1),
    }
    parent = elem.add_element("matches", matches_attrs)
    if by_rmid.size > 0
      by_rmid.sort.each{|id, values|
        match_attrs = {
          "rmid"			=> id,
          "externalId"			=> @misc[:by_rmid][id][:pure_rec_ids][:externalId],
          "uuid"			=> @misc[:by_rmid][id][:pure_rec_ids][:uuid],
          "pureId"			=> @misc[:by_rmid][id][:pure_rec_ids][:pureId],
          "researchOutput"		=> @misc[:by_rmid][id][:pure_rec_name],
        }
        child = parent.add_element("match", match_attrs)
        grandchild = child.add_element("pure_refs")
        @misc[:by_rmid][id][:pure_rec_other_ids].each{|n,v|
          grandchild.add_element("pure_ref", {n.to_s => v})
        }

        case @@how_to_match
        when :DspaceDoiToPureRmid
          grandchild = child.add_element("matching_doi_refs")
          values[:matched_dois].sort.each{|doi| grandchild.add_element("matching_doi_ref", "doi"=>doi)}

        when :DspaceRmidToPureRmid
          grandchild = child.add_element("matching_rmid_refs")
          values[:matched_rmids].sort.each{|rmid| grandchild.add_element("matching_rmid_ref", "rmid"=>rmid)}
        end
      }
    end
  end

  ############################################################################
  # Look for Creative Commons abbreviation (eg. CC-BY-SA) or CC URL
  # (eg. https://creativecommons.org/licenses/by-sa/...)
  #
  # - Within abbreviations, cope with "CCBY" (without space or hyphen)
  # - Within both abbreviations & URL, cope with newlines
  def get_licence_step1_per_item
    a = @dc[:description] + @dc[:rights]
    a.each{|desc|
      # It is important to test for abbreviations in the correct order
      # (as per the LICENCE_KEYS array).
      LICENCE_KEYS.each{|k| return k.to_s.upcase.gsub("_", "-") if
        desc.match(LICENCE_ABBR_REGEX_LIST[k]) || desc.match(LICENCE_URL_REGEX_LIST[k])}
    }
    nil
  end

  ############################################################################
  def get_licence_from_authority
    if @dc[:license].empty?
      STDERR.puts "WARNING: #{__method__}: No licence"

    else
      @dc[:license].each{|desc|
        unless LICENCE_ABBR_TARGETS.include?(desc)
          # FIXME: Verify with real data
          STDERR.puts "WARNING: #{__method__}: Unexpected licence '#{desc}'"
        end
        return desc
      }
    end
    nil
  end

  ############################################################################
  # If no match for Creative Commons licence at item-level, try the
  # following at the bitstream-level:
  #   IF 'Author version' AND Publisher is NOT Elsevier THEN
  #     license = 'In Copyright'
  #   ENDIF
  def get_licence_step2_per_bitstream(bitstream_obj)
    if @misc[:itemlicence] && !@misc[:itemlicence].empty?
      @misc[:itemlicence]

    elsif bitstream_obj[:docversion] == "author" && @misc[:publisher_elsevier].nil?
      "In Copyright"

    else
      # FIXME: Issue warning if no licence?
      nil
    end
  end

  ############################################################################
  def get_clean_id(id_type)
    unless CLEAN_ID_TYPES.values.include?(id_type)
      STDERR.puts "ERROR: Expected id_types #{CLEAN_ID_TYPES.values.inspect} but got '#{id_type}'."
      exit(10)
    end

    ids = []
    id_msgs = []
    return [ids, id_msgs.join("; ")] if @dc[id_type].empty?
    ids = @dc[id_type].inject([]){|a,s_id|
      s = id_type==:doi ?
            s_id.gsub(DOI_DEL_URL_REGEX, "").strip :	# DOI in non-URL format
            s_id.strip
      s.empty? ? a : a << s
    }

    prev_num_ids = ids.length
    ids.uniq!				# Remove duplicate IDs
    s_id_type = id_type.to_s.upcase
    id_msgs << "De-dupped #{s_id_type}s" if ids.length != prev_num_ids
    id_msgs << "More than 1 #{s_id_type}" if ids.length > 1
    [ids, id_msgs.join("; ")]
  end

  ############################################################################
  # Look for Elsevier in dc.publisher (& dc.rights & dc.description)
  def get_publisher_elsevier
    # Must be the publisher if found in dc.publisher
    @dc[:publisher].each{|p| return "Elsevier" if p.match(ELSEVIER_REGEX)}

    # Might be the publisher if found in dc.description or dc.rights
    a = @dc[:description] + @dc[:rights]
    a.each{|p| return "[Elsevier???]" if p.match(ELSEVIER_REGEX)}
    nil
  end

  ############################################################################
  def process_grant(funder, grant_num, grant_info, gwarnings, dc_field_name, grant_info_this_dc, grant_info_all_prev_dc={})
    unless FUNDERS.include?(funder)
      gwarnings << "#{dc_field_name} unexpected funder"
      return false	# Return failure
    end

    grant_num.match(/NOT\s*FOUND/i)
    if Regexp.last_match
      gwarnings << "#{dc_field_name} unexpected grant number '#{grant_num}'"
      return false	# Return failure
    end

    # Don't add duplicate funder+grant_num
    key_grant_ref = "#{funder}/#{grant_num}"
    if grant_info_this_dc.has_key?(key_grant_ref)
      gwarnings << "#{dc_field_name} duplicate"
    else
      grant1 = [key_grant_ref, "#{PURL_PREFIX}/#{key_grant_ref}"]
      grant_info_this_dc[key_grant_ref] = grant1
      grant_info << grant1 unless grant_info_all_prev_dc.has_key?(key_grant_ref)
    end
    true		# Return success
  end

  ############################################################################
  # For ARC & NHMRC grants, get list of [funder, grant_num, purl].
  def get_grant_info
    # See https://help.nla.gov.au/trove/becoming-partner/for-content-partners/adding-NHMRC-ARC
    # - Funder:    Upper case for consistency (in both Funder & PURL)
    # - Grant Num: Upper case for consistency (in both Grant Num & PURL)
    grant_info = []			# Ordered list of unique funders & grant numbers
    gwarnings = []			# List of warnings

    grant_info_rel = {}			# Hash for dc.relation
    @dc[:relation].each{|s|		# Format: URL_PREFIX/funder/grant_num
      s.strip.match(PURL_REGEX)
      if [$1, $2, $3].all?{|o| !o.to_s.empty?}
        funder, grant_num = $2.upcase, $3.upcase
        next unless process_grant(funder, grant_num, grant_info, gwarnings, "dc.relation", grant_info_rel)

      else
        gwarnings << "dc.relation purl format"
      end
    }

    grant_info_gn = {}			# Hash for dc.relation.grantnumber
    @dc[:grantnumber].each{|s|		# Format: funder/grant_num
      s.strip.match(FUNDER_GRANTNUM_REGEX)
      if [$1, $2].all?{|o| !o.to_s.empty?}
        funder, grant_num = $1.upcase, $2.upcase
        next unless process_grant(funder, grant_num, grant_info, gwarnings, "dc.relation.grantnumber", grant_info_gn, grant_info_rel)

      else
        gwarnings << "dc.relation.grantnumber format"
      end
    }

    gwarnings << "dc.relation/dc.relation.grantnumber grants differ" unless grant_info_rel == grant_info_gn
    [gwarnings, grant_info]
  end

  ############################################################################
  # A Pure research-output record may have more than 1 rmid.
  # - One will be the externalId attribute of the root element
  # - Others may be under the XPath
  #   /ROOT/info/additionalExternalIds/id[@idSource="researchoutputwizard"]
  # If 2 RMIDs match 1 record due to the above property, this method ensures
  # only 1 record (hence rmid) is listed as a match.
  def dedup_pure_rec
    return if @misc[:by_rmid].size < 2	# Dups not possible for 0 or 1 rmids

    rmids = {}
    is_preferred_rmids = {}
    @misc[:by_rmid].each{|rmid,values|
      uuid = values[:pure_rec_ids][:uuid]
      rmids[uuid] ||= []
      rmids[uuid] << rmid

      is_preferred_rmids[uuid] ||= []
      is_preferred_rmids[uuid] << (rmid == values[:pure_rec_ids][:externalId])
    }
    return if rmids.all?{|uuid,id_list| id_list.size < 2}	# 1 rmid for each uuid

    # At least 1 record (uuid) needs to be deduped by discarding some rmids.
    is_preferred_rmids.each{|uuid,is_prefs|
      i = is_prefs.find_index{|is_pref| is_pref}
      rmid_keep = i ? rmids[uuid][i] : rmids[uuid][0]
      rmids_to_del = rmids[uuid] - [rmid_keep]
      STDERR.puts "INFO: For uuid #{uuid} & rmid #{rmid_keep}; deleting matching-rmids #{rmids_to_del.inspect}"
      rmids_to_del.each{|id| @misc[:by_rmid].delete(id)}
    }
  end

  ############################################################################
  def do_match
    case @@how_to_match
    when :DspaceDoiToPureRmid
      match_rmid_by_doi

    when :DspaceRmidToPureRmid
      match_rmid_direct
    end
  end

  ############################################################################
  def match_rmid_by_doi
    # @misc[:by_rmid][RMID1][:matched_dois] = [DOI1a, DOI1b, ...]
    @misc[:by_rmid] = {}

    @misc[:doi_clean].each{|doi|
      if @@rmids_by_doi[doi]
        STDERR.puts "#{__method__.to_s.upcase}: #{doi}|#{@@rmids_by_doi[doi].inspect}"

        @@rmids_by_doi[doi].each{|id|
          @misc[:by_rmid][id] ||= {}
          @misc[:by_rmid][id][:matched_dois] ||= []
          @misc[:by_rmid][id][:matched_dois] << doi
        }
      end
    }
    @misc[:by_rmid].each{|rmid,_| get_pure_rec_ids(rmid)}
    dedup_pure_rec
  end

  ############################################################################
  def match_rmid_direct
    # For compatibility with match_rmid_by_doi(),
    #   @misc[:by_rmid][RMID1][:matched_rmids] = [RMID1]
    @misc[:by_rmid] = {}

    @misc[:rmid_clean].each{|rmid|
      if @@rmids[rmid]
        STDERR.puts "#{__method__.to_s.upcase}: #{rmid}"

        @misc[:by_rmid][rmid] = {
          :matched_rmids => [rmid],
        }
      end
    }
    @misc[:by_rmid].each{|rmid,_| get_pure_rec_ids(rmid)}
    dedup_pure_rec
  end

  ############################################################################
  def get_derived_info
    unless File.readable?(calc_pathname(:pkg))
      STDERR.puts "ERROR: Cannot find DSpace package file: #{calc_pathname(:pkg)}"
      exit(8)
    end

    # Dublin Core info used in CSV or deriving other info
    # eg. itemlicence, publisher_elsevier, grant_info.
    # Initialise the @dc (Dublin Core) hash
    ##dc_elem_keys = [:description, :doi, :publisher, :rights, :grantnumber, :relation, :title]
    dc_elem_keys = XPATH_DC.keys
    @dc = dc_elem_keys.inject({}){|h,k| h[k] = []; h}

    # Populate the @dc hash
    File.open(calc_pathname(:pkg)){|f|
      pdoc = REXML::Document.new(f)
      dc_elem_keys.each{|k|
        pdoc.elements.each(XPATH_DC[k]){|e| @dc[k] << e.text}
      }
    }

    @misc = {}
    # Item-level licence
    @misc[:itemlicence] = get_licence_step1_per_item
    @misc[:publisher_elsevier] = get_publisher_elsevier
    @misc[:grant_warnings], @misc[:grant_info] = get_grant_info
    @misc[:doi_clean],  @misc[:doi_msg]  = get_clean_id(:doi)
    @misc[:rmid_clean], @misc[:rmid_msg] = get_clean_id(:rmid)

    # FIXME: Test this with deleted bitstreams - cannot find any.
    attrs_bs = @attrs[:bitstream]
    @misc[:num_total] = attrs_bs.nil? ? 0 : attrs_bs.size
    @misc[:num_deleted] = if attrs_bs.nil?
      0
    else
      attrs_bs.inject(0){|sum,a| sum+=1 unless a[:deleted] == "false"; sum}
    end
    @misc[:num_undeleted] = @misc[:num_total] - @misc[:num_deleted]

    # Bitstream-level licence
    if @misc[:num_undeleted] > 0	# Some (undeleted) bitstreams for this item
      @attrs[:bitstream].each{|bs|
        next unless bs[:deleted] == "false"
        bs[:doclicence_draft] = get_licence_step2_per_bitstream(bs)
        bs[:doclicence] = get_licence_from_authority
      }
    end
  end

  ############################################################################
  # Item level info
  def csv_line_part1
    [
      @item_id,
      @item[:url],
    ]
  end

  ############################################################################
  # Item level info
  def csv_line_part3
    rmids = @misc[:by_rmid].keys.sort
    [
      @misc[:itemlicence],
      @misc[:publisher_elsevier],

      @misc[:grant_warnings].join(MULTIVALUE_DELIM),
      @misc[:grant_info].inject([]){|a,(grant_ref,_)| a << grant_ref; a}.join(MULTIVALUE_DELIM),
      @misc[:grant_info].inject([]){|a,(_,grant_purl)| a << grant_purl; a}.join(MULTIVALUE_DELIM),

      @misc[:doi_clean].join(MULTIVALUE_DELIM),
      @misc[:doi_msg],
      @misc[:rmid_clean].join(MULTIVALUE_DELIM),
      @misc[:rmid_msg],

      "by_#{CLEAN_ID_TYPES[@@how_to_match]}",	# match_by
      (@misc[:by_rmid].size == 1).inspect,	# uniq_match
      rmids.join(MULTIVALUE_DELIM),		# rmids_match
      rmids.map{|id| @misc[:by_rmid][id][:pure_rec_name]}.join(MULTIVALUE_DELIM), # rec_names_match

      @dc[:publisher].join(MULTIVALUE_DELIM),
      @dc[:grantnumber].join(MULTIVALUE_DELIM),
      @dc[:relation].join(MULTIVALUE_DELIM),
      @dc[:rights].join(MULTIVALUE_DELIM),
      @dc[:description].join(MULTIVALUE_DELIM),
      @dc[:title].join(MULTIVALUE_DELIM),
    ]
  end

  ############################################################################
  def save_csv_line(csv_out, csv_out_omit)
    this_csv = will_omit_item ? csv_out_omit : csv_out
    info_for_csv_lines.each{|csv_line|
      this_csv << csv_line
    }
  end

  ############################################################################
  # One CSV line per bitstream (not per item)
  def info_for_csv_lines
    # FIXME: Decode from XML; encode for CSV

    # FIXME: Note this condition in the XML file doco
    if ITEM_STATUS_OK.any?{|k,v| @item[k]!=v}	# Omit
      STDERR.puts "INFO: Item is withdrawn, hidden or incomplete; not writing to CSV"
      return []
    end

    if DEBUG
      STDERR.puts "@attrs[:bitstream]=#{@attrs[:bitstream].inspect}"
      STDERR.puts "licence: #{@misc[:itemlicence]}"
      STDERR.puts "publisher_elsevier: #{@misc[:publisher_elsevier]}"
      STDERR.puts "grant_info: #{@misc[:grant_info].inspect}"
      STDERR.puts "num_total=#{@misc[:num_total]}; num_deleted=#{@misc[:num_deleted]}; num_undeleted=#{@misc[:num_undeleted]}"
    end

    csv_lines = []
    if @misc[:num_undeleted] == 0	# No (undeleted) bitstreams for this item
        csv_lines << (csv_line_part1 + [
          @misc[:num_undeleted],
          @misc[:num_deleted],

          "",
          "",
          "",
          "",
        ] + csv_line_part3)

    else			# One or more (undeleted) bitstreams for this item
      @attrs[:bitstream].each{|bs|
        next unless bs[:deleted] == "false"
        csv_lines << csv_line_part1 + [
          @misc[:num_undeleted],
          @misc[:num_deleted],

          bs[:fname],
          bs[:docversion],
          bs[:doclicence_draft],
          bs[:doclicence],
          bs[:deleted],
        ] + csv_line_part3
      }
    end
    STDERR.puts "csv_lines:#{csv_lines.inspect}" if DEBUG
    csv_lines
  end

  ############################################################################
  def self.get_csv_header_line
    %w{
      item_id
      hdl_url

      nFilesExist
      nFilesDel
      fname
      docversion
      doclicence_draft
      doclicence
      docdeleted

      itemlicence
      elsevier

      grant_warnings
      grant_ref
      grant_purl

      dois_clean
      doi_msg
      rmids_clean
      rmid_msg

      match_by
      uniq_match
      rmids_match
      rec_names_match

      dc_publisher
      dc_relation_grantnumber
      dc_relation
      dc_rights
      dc_description
      dc_title
    }
  end

  ############################################################################
  def self.initialise_is_open_access
    cmd = "hostname"
    STDERR.puts "Command: #{cmd}"
    output = %x{ #{cmd} }		# Execute OS command
    res = $?

    unless res.to_s == "0"
      STDERR.puts "ERROR: Return code #{res} when executing command:\n  #{cmd}"
      exit(7)
    end

    @@is_open_access = !output.match(OPEN_ACCESS_HOST_REGEX).nil?
    STDERR.puts "INFO: *%s* bitstreams are open access on this host" % (@@is_open_access ? "ALL" : "NO")
  end

  ############################################################################
  def self.initialise_match_rmid_by_doi
    STDERR.puts "INFO: BEGIN #{__method__}"
    opts = {
      :col_sep => '|',
      :headers => false,
      :force_quotes => false,
    }
    @@rmids_by_doi = {}
    FasterCSV.foreach(FPATH_CSV_IN_ID_DOI, opts){|fields| 
      if fields.size == 2
        id = fields[0].to_s.strip
        doi = fields[1].to_s.strip
        if !id.empty? && !doi.empty?
          @@rmids_by_doi[doi] ||= []
          @@rmids_by_doi[doi] << id
        else

          STDERR.puts "ERROR: One of the RMID/DOI fields is empty in #{fields.inspect}"
        end

      else
        STDERR.puts "ERROR: Expecting 2 fields in #{fields.inspect}"
      end
    }
    STDERR.puts "INFO: END #{__method__}"
  end

  ############################################################################
  def self.initialise_match_rmid_direct
    STDERR.puts "INFO: BEGIN #{__method__}"
    opts = {
      :col_sep => '|',
      :headers => false,
      :force_quotes => false,
    }
    @@rmids = {}
    FasterCSV.foreach(FPATH_CSV_IN_RMIDS, opts){|fields| 
      if fields.size == 1
        id = fields[0].to_s.strip
        if !id.empty?
          @@rmids[id] = true
        else

          STDERR.puts "ERROR: The RMID field (line) is empty in #{fields.inspect}"
        end

      else
        STDERR.puts "ERROR: Expecting 1 field in #{fields.inspect}"
      end
    }
    STDERR.puts "INFO: END #{__method__}"
  end

  ############################################################################
  def self.initialise_match
    STDERR.puts "INFO: How to match: #{@@how_to_match}"
    case @@how_to_match
    when :DspaceDoiToPureRmid
      initialise_match_rmid_by_doi

    when :DspaceRmidToPureRmid
      initialise_match_rmid_direct

    else
      STDERR.puts "ERROR: Expected How to match to be one of #{HOW_TO_MATCH_LIST.inspect}. Got #{@@how_to_match.inspect}."
      exit(9)
    end
  end

  ############################################################################
  def self.set_how_to_match
    @@how_to_match = HOW_TO_MATCH
  end

  ############################################################################
  def self.get_item_id_from_item_obj(item_obj, batch_type)
    item_id, descr = if batch_type == :item_id
      [item_obj, nil]

    else
      item_obj
    end

    STDERR.puts "\n### item_id='#{item_id}'%s" % (descr ? " -- #{descr}" : "")
    item_id
  end

  ############################################################################
  def self.warn_if_xml_dirs_not_empty
    [DIR_DSPACE_OUT, DIR_DSPACE_OUT_OMIT].each{|dir|
      if File.directory?(dir)
        num_files = (Dir.entries(dir) - %w{. ..}).size	# Ignore self-dir & parent-dir
        STDERR.puts "WARNING: Directory #{dir} is not empty (contains #{num_files} files/dirs)." if num_files > 0
      end
    }
  end

  ############################################################################
  def self.process_item_batch(item_batch, batch_type)
    require 'item_ids_omit'	# ItemIdsOmit.item_list() has list of item_ids to exclude
    initialise_is_open_access
    initialise_match
    warn_if_xml_dirs_not_empty
    FileUtils.mkdir_p(DIR_RESULTS)	# Dir containing CSV files

    # Open CSV files for output
    FasterCSV.open(FPATH_CSV_OUT, "w", FCSV_OUT_OPTS){|csv_out| 
      FasterCSV.open(FPATH_CSV_OUT_OMIT, "w", FCSV_OUT_OPTS){|csv_out_omit| 
        csv_out << get_csv_header_line
        csv_out_omit << get_csv_header_line

        item_batch.each{|item_obj|
          item_id = get_item_id_from_item_obj(item_obj, batch_type)
          item = Item2Export.new(item_id)
          begin
            item.get_item_from_db
            item.get_item_package
            item.slice_into_attrs
            item.get_derived_info
            item.do_match

            item.create_custom_xml
            item.save_custom_xml
            item.save_csv_line(csv_out, csv_out_omit)

          rescue Exception => e
            STDERR.puts "ERROR item_id:'#{item_id}' -- #{e.inspect}"
          end
        }
      }
    }
  end

  ############################################################################
  # The main method for this class
  ############################################################################
  def self.main
    STDERR.puts "\nExporting items in XML format (#{__method__})"
    STDERR.puts   "-----------------------------"

    require 'item_ids'		# ItemIds.item_list() has list of item_ids
    process_item_batch(ItemIds.item_list, :item_id)
  end

  ############################################################################
  # The main TEST method for this class
  ############################################################################
  def self.main_test
    STDERR.puts "\nTEST: Exporting items in XML format (#{__method__})"
    STDERR.puts   "-----------------------------------"

    item_info_list = [
    #  item_id,	description

      [27215,	"123456789/26806 ; 2 bitstreams; 2 diff licences; omit item"],

      [31900,	"123456789/31615 ; 0 bitstreams; no dc.publisher & no dc.descr; rmid-match"],
      [10149,	"123456789/10032 ; 1 bitstream; no dc.publisher but Elsevier & CC-BY-NC-ND"],
      [37670,	"123456789/37250 ; 2 bitstreams; CC BY & Pub version"],
      [39722,	"123456789/39239 ; bitstream embargo; Elsevier & Author version"],
      [35759,	"123456789/35433 ; item embargo (past lift 2018-03-25); Oxford UP & dc.descr; grantnumber"],
      [38703,	"123456789/38236 ; item embargo (future lift 2021-07-27); metadata not visible"],

      [39049,	"123456789/38577 ; 1 bitstream; dc.relation & grantnumber"],
      [36988,	"123456789/36557 ; 1 bitstream; dc.relation & grantnumber; CC-BY url in dc.description"],
      [27052,	"123456789/26672 ; 1 bitstream; 3x dc.relation & 3x grantnumber"],

      [35776,	"123456789/35756 ; 1 bitstream; 1x dc.relation & 3x grantnumber"],
      [37875,	"123456789/37455 ; 1 bitstream; 4x dc.relation (dup) & 2x grantnumber; CC-BY url in dc.description"],
      [27689,	"123456789/27485 ; 0 bitstream; 3x dc.relation (error) & 2x grantnumber"],

      [26360,	"123456789/25948 ; withdrawn item"],
      [10758,	"123456789/10641 ; hidden item; rmid-match"],

      # Tests with CC-BY-* licences
      [38739,	"123456789/38271 ; X; CBY- NC-ND in dc.description & dc.rights; abbr + url"],
      [38738,	"123456789/38270 ; X; CCBY- NC-ND in dc.description & dc.rights; abbr + url"],
      [38733,	"123456789/38265 ; X; CCBY- NC-ND in dc.description & dc.rights; abbr + url; 1x DOI"],
      [39256,	"123456789/38794 ; X; 2x DOI"],
      [39614,	"123456789/39151 ; X; 2x DOI dup"],
      [39771,	"123456789/39299 ; X; 0x DOI"],
      [36412,	"123456789/36019 ; X; 1x DOI; 2x RMID; 1x UUID"],
      [27055,	"123456789/26675 ; X; 2x DOI; 2x RMID; 2x UUID"],
      [13120,	"123456789/13003 ; X; MIME type jpg; rmid-match"],

      [38639,	"123456789/38168 ; X; CCBY- NC-ND in dc.rights; abbr + url"],
      [38117,	"123456789/37695 ; X: CCBY in dc.description; abbr + url"],
      [37494,	"123456789/37071 ; X; CCBY- NC-ND in dc.description; abbr"],
      [39782,	"123456789/39305 ; No licence & auth ver & not Elsevier => In Copyright"],
      [39721,	"123456789/39238 ; No licence & publ ver & not Elsevier => No licence"],
      [39724,	"123456789/39241 ; No licence & auth ver & Elsevier => No licence"],
      [35880,	"123456789/35536 ; No licence & publ ver & Maybe-Elsevier => No licence"],
=begin
=end

      # Attempt to find bitstream with deleted='true'. Neither of those below.
      #[7315,	"123456789/8435  ; Previously missing bitstream"],
      #[39771,	"123456789/39299 ; Deleted bitstream in Checksum Checker Report"],

    ]
    process_item_batch(item_info_list, :item_id_descr)
  end

end

##############################################################################
# Main
##############################################################################
Item2Export.set_how_to_match

Item2Export.main_test
#Item2Export.main
exit(0)

