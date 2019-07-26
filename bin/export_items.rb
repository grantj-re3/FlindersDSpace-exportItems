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
#
#++
##############################################################################

# Add dirs to the library path
$: << File.expand_path("../lib", File.dirname(__FILE__))
$: << File.expand_path(".", File.dirname(__FILE__))
$: << "#{ENV['HOME']}/.ds/etc"

require 'date'
require "rexml/document"
require 'rexml/xpath'
require 'fileutils'
require 'pp'

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
    # Sort attributes by name
    alias _each_attribute each_attribute
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

  ############################################################################
  def initialize(item_id)
    @item_id = item_id			# DSpace item_id
    @item = nil				# Item info from DB
    @doc = nil				# XML document

    @_bundle_embargo_attrs = nil	# Cache for bundle_embargo_attrs()
  end

  ############################################################################
  def bitstream_text_value_clause(element_name)
    # For element_name 'title', clause will extract bitstream filename
    # For element_name 'description', clause will extract bitstream file description
    <<-SQL_BITSTREAM_TEXT_VALUE_CLAUSE.gsub(/^\t*/, '')
	      (select text_value from metadatavalue where resource_type_id=0 and resource_id=b.bitstream_id and metadata_field_id in
	        (select metadata_field_id from metadatafieldregistry where qualifier is null and element='#{element_name}')) 
    SQL_BITSTREAM_TEXT_VALUE_CLAUSE
  end

  ############################################################################
  def get_item_sql_query
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
	    from resourcepolicy p where p.resource_type_id=#{RESOURCE_TYPE_IDS[:item]} and p.resource_id=i.item_id
	  ), '||') item_policies,

	  (select resource_id || '^' || policy_id || '^' || action_id || '^' ||
             (case when start_date is null then '' else to_char(start_date, 'YYYY-MM-DD') end) || '^#{bundle_title}'
	   from resourcepolicy p where p.resource_type_id=#{RESOURCE_TYPE_IDS[:bundle]} and p.resource_id=
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
            where p.resource_type_id=#{RESOURCE_TYPE_IDS[:bitstream]} and p.resource_id=b.bitstream_id and b.deleted='f' and b.bitstream_id in
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
  def item_embargo_attrs(s_item_policy)
    sf = {}					# DB subfields
    sf[:item_id], sf[:policy_id], sf[:action_id], sf[:start_date] = s_item_policy.split(SUBFIELD_DELIM)
    unless sf[:action_id] == POLICY_ACTION_IDS[:read].to_s
      puts "ERROR: For item_id #{item_id}, expected action_id of #{POLICY_ACTION_IDS[:read]} but got #{sf[:action_id]}."
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
    attrs.inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attrs-key is a string
  end

  ###########################################################################
  def bundle_embargo_attrs
    return nil unless @item[:bundle_policy]
    return @_bundle_embargo_attrs if @_bundle_embargo_attrs	# Return cached object

    sf = {}					# Subfields
    sf[:bundle_id], sf[:policy_id], sf[:action_id], sf[:start_date], sf[:bundle_title] = @item[:bundle_policy].split(SUBFIELD_DELIM)
    unless sf[:action_id] == POLICY_ACTION_IDS[:read].to_s
      puts "ERROR: For bundle_id #{sf[:bundle_id]}, expected action_id of #{POLICY_ACTION_IDS[:read]} but got #{sf[:action_id]}."
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
    @_bundle_embargo_attrs = attrs.inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attrs-key is a string
  end

  ###########################################################################
  def bitstream_embargo_attrs(s_bitstream_policy)
    sf = {}					# Subfields
    sf[:bitstream_id], sf[:policy_id], sf[:action_id], sf[:start_date],
      sf[:deleted], sf[:seq], sf[:bytes], sf[:internal_id] = s_bitstream_policy.split(SUBFIELD_DELIM)
    unless sf[:action_id] == POLICY_ACTION_IDS[:read].to_s
      puts "ERROR: For bitstream_id #{sf[:bitstream_id]}, expected action_id of #{POLICY_ACTION_IDS[:read]} but got #{sf[:action_id]}."
      exit(4)
    end
    sf[:fpath] = DSPACE_ASSETSTORE_DIRPATH + sf[:internal_id].sub(/^((\d\d)(\d\d)(\d\d)(.*))$/, '\2/\3/\4/\1')

    # FIXME: Process: :deleted
    attrs = {}					# XML attributes
    [
      :bitstream_id,
      :policy_id,
      :action_id,

      :deleted,
      :seq,
      :bytes,
      :fpath,
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
    attrs.inject({}){|h,(k,v)| h[k.to_s]=v; h}	# Ensure attrs-key is a string
  end

  ############################################################################
  def create_custom_xml
    @doc = REXML::Document.new "<dspace_item><custom/></dspace_item>"
    #@doc << REXML::XMLDecl.new

    e = REXML::XPath.first(@doc, "//custom")
    e.add_element("debug_db_info", debug_db_info_attrs)	# Database debug info
    e.add_element("item_ids", item_ids_attrs)
    e.add_element("item_status", item_status_attrs)

    # Add embargo tree; XPath /dspace_item/custom/item_embargo/bundle_embargo/bitstream_embargo
    STDERR.printf "\nEMBARGO_REF_DATE                     : %s\n", EMBARGO_REF_DATE.strftime(DEBUG_DATE_FMT) if DEBUG
    @item[:item_policies].split(MULTIVALUE_DELIM).each{|ip|
      e.add_element("item_embargo", item_embargo_attrs(ip))
    }

    if bundle_embargo_attrs
      e = REXML::XPath.first(@doc, "//item_embargo[last()]")
      e.add_element("bundle_embargo", bundle_embargo_attrs)

      e = REXML::XPath.first(@doc, "//bundle_embargo")
      @item[:bitstream_policies].split(MULTIVALUE_DELIM).each{|bsp|
        e.add_element("bitstream_embargo", bitstream_embargo_attrs(bsp))
      }
    end

    STDERR.puts "Get & add DSpace XML package to custom XML" if DEBUG
    get_item_package
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
  # The main method for this class
  ############################################################################
  def self.main
    STDERR.puts "\nExporting items in XML format"
    STDERR.puts   "-----------------------------"

    item_info_list = [
    #  item_id,	description
      [38703,	"123456789/38236 ; item embargo (future lift 2021-07-27)"],
=begin
      [35759,	"123456789/35433 ; item embargo (past lift 2018-03-25)"],
      [39722,	"123456789/39239 ; bitstream embargo"],
      [31900,	"123456789/31615 ; 0 bitstreams"],
      [10149,	"123456789/10032 ; 1 bitstream"],
      [37670,	"123456789/37250 ; 2 bitstreams"],
=end
    ]

    item_info_list.each{|item_id, descr|
      STDERR.puts "\n### item_id='#{item_id}' -- #{descr}"
      item = Item2Export.new(item_id)
      begin
        item.get_item_from_db
        item.create_custom_xml
        item.save_custom_xml
        # FIXME:
        # - In the output dir, add bitstream symlink (into assetstore)
        # - Consider adding an ID (within XML) which matches the other system

      rescue Exception => e
        STDERR.puts "ERROR item_id:'#{item_id}' -- #{e.inspect}"
      end
    }
  end

end

##############################################################################
# Main
##############################################################################
Item2Export.main
exit(0)

