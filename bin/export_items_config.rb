#
# Copyright (c) 2019, Flinders University, South Australia. All rights reserved.
# Contributors: Library, Corporate Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#
# Config vars for ruby
##############################################################################
module Item2ExportConfig
  DEBUG = true
  DEBUG_DATE_FMT = "%Y-%m-%d %H:%M:%S %Z"

  # Force the extraction of the DSpace item package (using DSPACE_PKG_CMD_PART1
  # below) even if the resulting file already exists. Setting to false gives a
  # performance improvement *if* you've already extracted some or all of the
  # item packages.
  FORCE_GET_ITEM_PKG = false

  # Database multi-value field & subfield delimiters
  # Combined usage: "subfldA1^subfldA2^subfldA3||subfldB1^subfldB2^subfldB3"
  MULTIVALUE_DELIM = "||"	# Usage: "fldA||fldB"
  SUBFIELD_DELIM = "^"		# Usage: "subfldA1^subfldA2^subfldA3"

  # Files & dirs
  DIR_TOP = File.expand_path("..", File.dirname(__FILE__))
  DIR_RESULTS = "#{DIR_TOP}/results"
  DIR_BIN = "#{DIR_TOP}/bin"
  FPATH_CSV_OUT = "#{DIR_BIN}/dataReview.csv"

  DIR_DSPACE_PACKAGE = "#{DIR_TOP}/results/aip"
  BASENAME_DSPACE_PACKAGE = "aip_"

  DIR_DSPACE_OUT = "#{DIR_TOP}/results/out"
  BASENAME_DSPACE_OUT = "out_"

  # DSpace parameters
  DSPACE_ASSETSTORE_DIRPATH = "/file/path/to/dspace/assetstore/"

  HANDLE_URL_LEFT_STRING = 'https://dspace.example.com/xmlui/handle/'

  DSPACE_EXE = "/file/path/to/dspace/bin/dspace"
  DSPACE_EPERSON4PACKAGER = "admin_user@example.com"	# Email address of the E-Person under whose authority this runs
  DSPACE_PKG_CMD_PART1 = "#{DSPACE_EXE} packager --disseminate --type AIP --option manifestOnly=true --eperson #{DSPACE_EPERSON4PACKAGER}"

  # XPath to various Dublin Core elements within DSpace AIP package
  XPATH_DC_PREFIX = "/mets/dmdSec/mdWrap/xmlData/dim:dim/dim:field"
  XPATH_DC = {
    :description	=> "#{XPATH_DC_PREFIX}[@element='description'][not(@qualifier)]",
    :publisher		=> "#{XPATH_DC_PREFIX}[@element='publisher'][not(@qualifier)]",
    :rights		=> "#{XPATH_DC_PREFIX}[@element='rights'][not(@qualifier)]",
    :relation		=> "#{XPATH_DC_PREFIX}[@element='relation'][not(@qualifier)]",
    :grantnumber	=> "#{XPATH_DC_PREFIX}[@element='relation'][@qualifier='grantnumber']",
    :title		=> "#{XPATH_DC_PREFIX}[@element='title'][not(@qualifier)]",
  }

  # Regular expressions for detecting various parameters
  ELSEVIER_REGEX = /(^|\W)elsevier($|\W)/i

  PURL_MID = "purl.org/au-research/grants"
  PURL_PREFIX = "http://#{PURL_MID}"
  PURL_REGEX = /(#{PURL_MID}\/(\S+)\/(\S+))/i

  # We are only interested in grants by these funders
  FUNDERS = %w{ARC NHMRC}
  FUNDER_GRANTNUM_REGEX = /^([\w\s]+)\/(\w+)$/

  # The order of these keys is important because they will be used
  # for matching, and the first match wins! Hence:
  # - cc_by must be last (ie. appear after cc_by_*)
  # - cc_by_nc must appear after cc_by_nc_*
  LICENCE_KEYS = %w{
    cc_by_sa
    cc_by_nd
    cc_by_nc_sa
    cc_by_nc_nd
    cc_by_nc
    cc_by
  }.map{|s| s.to_sym}

  # Regexs for matching licence abbreviations
  # - Permit zero or more hyphens and white-space chars (including newline)
  # - Beginning & end of match must not be an alpha char
  LICENCE_ABBR_K_V_LIST = LICENCE_KEYS.map{|k|
    v = '(^|[^[:alpha:]])' + k.to_s.gsub("_", "[\s-]*") + '($|[^[:alpha:]])'
    [k, Regexp.new(v, Regexp::IGNORECASE)]
  }
  LICENCE_ABBR_REGEX_LIST = Hash[LICENCE_ABBR_K_V_LIST]

  # Regexs for matching licence URLs
  # - Permit zero or more white-space chars (including newline) on either side of "/"
  LICENCE_URL_MID = "creativecommons.org/licenses/"
  LICENCE_URL_K_V_LIST = LICENCE_KEYS.map{|k|
    ks = k.to_s
    v = "#{LICENCE_URL_MID}#{ks[3...ks.size].gsub("_", "-")}/".gsub("/", '\s*/\s*')
    [k, Regexp.new(v, Regexp::IGNORECASE)]
  }
  LICENCE_URL_REGEX_LIST = Hash[LICENCE_URL_K_V_LIST]

  # FasterCSV options for writing CSV to output
  FCSV_OUT_OPTS = {
    :col_sep => ',',
    :headers => true,
    :force_quotes => true,
  }

  MAX_ITEMS_TO_PROCESS = 1000	# FIXME: Currently unused
end

