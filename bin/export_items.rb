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
  S_OK_ACTION_IDS = [:read, :withdrawn_read].map{|action| POLICY_ACTION_IDS[action].to_s}

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
	    select resource_id || '^' || policy_id || '^' || action_id || '^' ||
              (case when start_date is null then '' else to_char(start_date, 'YYYY-MM-DD') end)
	    from resourcepolicy p where p.epersongroup_id=#{EPERSON_GROUP_IDS[:public]} and p.resource_type_id=#{RESOURCE_TYPE_IDS[:item]} and p.resource_id=i.item_id
	  ), '||') item_policies,

	  (select resource_id || '^' || policy_id || '^' || action_id || '^' ||
             (case when start_date is null then '' else to_char(start_date, 'YYYY-MM-DD') end) || '^#{bundle_title}'
	   from resourcepolicy p where p.epersongroup_id=#{EPERSON_GROUP_IDS[:public]} and p.resource_type_id=#{RESOURCE_TYPE_IDS[:bundle]} and p.resource_id=
	     (select resource_id from metadatavalue where text_value='#{bundle_title}' and resource_type_id=#{RESOURCE_TYPE_IDS[:bundle]} and resource_id in
                #{bundle_clause}
	     )
	  ) bundle_policy,

	  array_to_string(array(
	    select resource_id || '^' || policy_id || '^' || action_id || '^' ||
	      (case when start_date is null then '' else to_char(start_date, 'YYYY-MM-DD') end) || '^' ||
	      deleted || '^' || sequence_id || '^' || size_bytes || '^' || internal_id || '^' ||
	      #{bitstream_text_value_clause('title')} || '^' ||
	      #{bitstream_text_value_clause('description')}
	    from resourcepolicy p, bitstream b
            where p.epersongroup_id=#{EPERSON_GROUP_IDS[:public]} and p.resource_type_id=#{RESOURCE_TYPE_IDS[:bitstream]} and p.resource_id=b.bitstream_id and b.deleted='f' and b.bitstream_id in
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
          puts "ERROR: Expected 1 but got #{length} rows for SQL query:"
          puts "#{sql}"
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
      puts "ERROR: For item_id #{@item_id}, expected action_id of #{S_OK_ACTION_IDS.join(',')} but got '#{sf[:action_id]}'."
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
      puts "ERROR: For bundle_id #{sf[:bundle_id]}, expected action_id of #{S_OK_ACTION_IDS.join(',')} but got '#{sf[:action_id]}'."
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
      sf[:fname], sf[:fdesc] = s_bitstream_policy.split(SUBFIELD_DELIM)
    # FIXME:
    # - Deal with policy action 12 (WITHDRAWN_READ) [item.withdrawn='t'] later.
    # - Consider adding <item_status state="ignore" reason="withdrawn"/> vs <item_status state="ok"
    # - Consider writing to different output dir?
    unless S_OK_ACTION_IDS.include?(sf[:action_id])
      puts "ERROR: For bitstream_id #{sf[:bitstream_id]}, expected action_id of #{S_OK_ACTION_IDS.join(',')} but got '#{sf[:action_id]}'."
      exit(4)
    end

    sf[:fpath] = DSPACE_ASSETSTORE_DIRPATH + sf[:internal_id].sub(/^((\d\d)(\d\d)(\d\d)(.*))$/, '\2/\3/\4/\1')
    sf[:docversion] = case sf[:fdesc].to_s
    when /author/i
      "author"
    when /publish/i
      "publisher"
    else
      "unknown"
    end

    # FIXME: Process: :deleted
    # FIXME: sf.keys - [:start_date, :internal_id]
    attrs = {}					# XML attributes
    [
      :bitstream_id,
      :policy_id,
      :action_id,

      :deleted,
      :seq,
      :bytes,

      :fpath,
      :fname,
      :fdesc,
      :docversion,
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
  def bitstream_embargo_attrs(h_bitstream_attrs)
    h_bitstream_attrs.inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attrs-key is a string
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

    STDERR.puts "Get & add DSpace XML package to custom XML" if DEBUG
    add_item_package_to_xml
  end

  ############################################################################
  def save_custom_xml
    fname_out = calc_pathname(:out)
    FileUtils.mkdir_p(File.dirname(fname_out))

    File.open(fname_out, "w"){|f|
      @doc.write(f, 2)
      f.puts
    }
  end

  ############################################################################
  def calc_pathname(type)
    unless @item && @item[:handle]
      puts "ERROR: Unable to determine file path for '#{type}'. Handle not found."
      exit(5)
    end

    fpart = @item[:handle].sub(/\//, "_")	# "111/2222" --> "111_2222"
    case type
      when :pkg
        "#{DIR_DSPACE_PACKAGE}/#{BASENAME_DSPACE_PACKAGE}#{fpart}.xml"

      when :out
        "#{DIR_DSPACE_OUT}/#{fpart}.d/#{BASENAME_DSPACE_OUT}#{fpart}.xml"

      else
        puts "ERROR: Invalid file path type '#{type}'."
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
      nil
    end
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
        unless FUNDERS.include?(funder)
          gwarnings << "dc.relation unexpected funder"
          next unless FUNDERS.include?(funder)
        end

        # Don't add duplicate funder+grant_num
        key_grant_ref = "#{funder}/#{grant_num}"
        if grant_info_rel.has_key?(key_grant_ref)
          gwarnings << "dc.relation duplicate"
        else
          grant1 = [key_grant_ref, "#{PURL_PREFIX}/#{key_grant_ref}"]
          grant_info_rel[key_grant_ref] = grant1
          grant_info << grant1
        end

      else
        gwarnings << "dc.relation purl format"
      end
    }

    grant_info_gn = {}			# Hash for dc.relation.grantnumber
    @dc[:grantnumber].each{|s|		# Format: funder/grant_num
      s.strip.match(FUNDER_GRANTNUM_REGEX)
      if [$1, $2].all?{|o| !o.to_s.empty?}
        funder, grant_num = $1.upcase, $2.upcase
        unless FUNDERS.include?(funder)
          gwarnings << "dc.relation.grantnumber unexpected funder"
          next unless FUNDERS.include?(funder)
        end

        # Don't add duplicate funder+grant_num
        key_grant_ref = "#{funder}/#{grant_num}"
        if grant_info_gn.has_key?(key_grant_ref)
          gwarnings << "dc.relation.grantnumber duplicate"
        else
          grant1 = [key_grant_ref, "#{PURL_PREFIX}/#{key_grant_ref}"]
          grant_info_gn[key_grant_ref] = grant1
          grant_info << grant1 unless grant_info_rel.has_key?(key_grant_ref)
        end

      else
        gwarnings << "dc.relation.grantnumber format"
      end
    }

    gwarnings << "dc.relation/dc.relation.grantnumber grants differ" unless grant_info_rel == grant_info_gn
    [gwarnings, grant_info]
  end

  ############################################################################
  def get_derived_info
    unless File.readable?(calc_pathname(:pkg))
      puts "ERROR: Cannot find DSpace package file: #{calc_pathname(:pkg)}"
      exit(8)
    end

    # Dublin Core info used in CSV or deriving other info
    # eg. itemlicence, publisher_elsevier, grant_info.
    # Initialise the @dc (Dublin Core) hash
    dc_elem_keys = [:description, :publisher, :rights, :grantnumber, :relation, :title]
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
        bs[:doclicence] = get_licence_step2_per_bitstream(bs)
      }
    end
  end

  ############################################################################
  def csv_line_part1
    [
      @item_id,
      @item[:url],
    ]
  end

  ############################################################################
  def csv_line_part3
    [
      @misc[:itemlicence],
      @misc[:publisher_elsevier],

      @misc[:grant_warnings].join(MULTIVALUE_DELIM),
      @misc[:grant_info].inject([]){|a,(grant_ref,_)| a << grant_ref; a}.join(MULTIVALUE_DELIM),
      @misc[:grant_info].inject([]){|a,(_,grant_purl)| a << grant_purl; a}.join(MULTIVALUE_DELIM),

      @dc[:publisher].join(MULTIVALUE_DELIM),
      @dc[:grantnumber].join(MULTIVALUE_DELIM),
      @dc[:relation].join(MULTIVALUE_DELIM),
      @dc[:rights].join(MULTIVALUE_DELIM),
      @dc[:description].join(MULTIVALUE_DELIM),
      @dc[:title].join(MULTIVALUE_DELIM),
    ]
  end

  ############################################################################
  def info_for_csv_line
    # FIXME: Decode from XML; encode for CSV
    # FIXME: Will old Ruby handle UTF8?
    # FIXME: Inject derived info into output XML
    # Extraction aims:
    # - from dc.description extract CC licence
    # - from dc.publisher extract Elsevier
    # - from dc.relation.grantnumber extract PURL + grant funder + grant ID
    # Generate CSV with columns:
    # - item_id, handle, dc.description, publisher/author version, publisher (Elsevier),
    #   dc.relation.grantnumber (PURL + grant funder + grant ID)

    # FIXME: Exclude from XML file?
    if @item[:withdrawn] == 't' || @item[:discoverable] == 'f'
      STDERR.puts "Item is withdrawn or hidden; not writing to CSV"
      return []
    end

    STDERR.puts "@attrs[:bitstream]=#{@attrs[:bitstream].inspect}"
    STDERR.puts "licence: #{@misc[:itemlicence]}"
    STDERR.puts "publisher_elsevier: #{@misc[:publisher_elsevier]}"
    STDERR.puts "grant_info: #{@misc[:grant_info].inspect}"
    STDERR.puts "num_total=#{@misc[:num_total]}; num_deleted=#{@misc[:num_deleted]}; num_undeleted=#{@misc[:num_undeleted]}"

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
          bs[:doclicence],
          bs[:deleted],
        ] + csv_line_part3
      }
    end
    STDERR.puts "csv_lines:#{csv_lines.inspect}"
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
      doclicence
      docdeleted

      itemlicence
      elsevier

      grant_warnings
      grant_ref
      grant_purl

      dc_publisher
      dc_relation_grantnumber
      dc_relation
      dc_rights
      dc_description
      dc_title
    }
  end

  ############################################################################
  def self.process_item_batch(item_batch, batch_type)
    # Open a CSV file for output
    FasterCSV.open(FPATH_CSV_OUT, "w", FCSV_OUT_OPTS){|csv_out| 
      csv_out << get_csv_header_line

      #item_info_list.each{|item_id, descr|
      item_batch.each{|item_obj|
        if item_obj == :item_id
          item_id = item_obj
          STDERR.puts "\n### item_id='#{item_id}'"
        else
          item_id, descr = item_obj
          STDERR.puts "\n### item_id='#{item_id}' -- #{descr}"
        end

        item = Item2Export.new(item_id)
        begin
          item.get_item_from_db
          item.get_item_package
          item.slice_into_attrs

          item.create_custom_xml
          # FIXME:
          # - In the output dir, add bitstream symlink (into assetstore)
          # - Consider adding an ID (within XML) which matches the other system
          item.save_custom_xml

          item.get_derived_info
          item.info_for_csv_line.each{|csv_line|
            csv_out << csv_line
          }

        rescue Exception => e
          STDERR.puts "ERROR item_id:'#{item_id}' -- #{e.inspect}"
        end
      }
    }
  end

  ############################################################################
  # The main method for this class
  ############################################################################
  def self.main
    STDERR.puts "\nExporting items in XML format (#{__method__})"
    STDERR.puts   "-----------------------------"

    require 'item_ids'              # ItemIds.item_list() has list of item_ids
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

      [31900,	"123456789/31615 ; 0 bitstreams; no dc.publisher & no dc.descr"],
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
      [10758,	"123456789/10641 ; hidden item"],

      # Tests with CC-BY-* licences
      [38739,	"123456789/38271 ; X; CBY- NC-ND in dc.description & dc.rights; abbr + url"],
      [38738,	"123456789/38270 ; X; CCBY- NC-ND in dc.description & dc.rights; abbr + url"],
      [38733,	"123456789/38265 ; X; CCBY- NC-ND in dc.description & dc.rights; abbr + url"],

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
Item2Export.main_test
#Item2Export.main
exit(0)

