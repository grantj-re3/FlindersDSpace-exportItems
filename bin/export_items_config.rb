#
# Copyright (c) 2019, Flinders University, South Australia. All rights reserved.
# Contributors: Library, Corporate Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#
# Config vars for ruby
##############################################################################
module Item2ExportConfig
  ### :DspaceDoiToPureRmid, :DspaceRmidToPureRmid
  HOW_TO_MATCH = :DspaceDoiToPureRmid

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

  # Item is ok for exporting if database attributes match those below
  ITEM_STATUS_OK = {
    :discoverable	=> 't',
    :in_archive		=> 't',
    :withdrawn		=> 'f',
  }

  # Files & dirs
  DIR_TOP = File.expand_path("..", File.dirname(__FILE__))
  DIR_RESULTS = "#{DIR_TOP}/results"
  DIR_BIN = "#{DIR_TOP}/bin"
  FPATH_CSV_IN_ID_DOI = "#{DIR_BIN}/pureRmDois.psv"
  FPATH_CSV_IN_RMIDS = "#{DIR_BIN}/pureRmids.psv"

  FPATH_CSV_OUT = "#{DIR_RESULTS}/dataReview.csv"
  FPATH_CSV_OUT_OMIT = "#{DIR_RESULTS}/dataOmit.csv"

  DIR_DSPACE_PACKAGE = "#{DIR_RESULTS}/aip"
  BASENAME_DSPACE_PACKAGE = "aip_"

  DIR_DSPACE_OUT = "#{DIR_RESULTS}/out"
  DIR_DSPACE_OUT_OMIT = "#{DIR_DSPACE_OUT}_omit"
  BASENAME_DSPACE_OUT = "out_"

  # Filepath components which point to Pure Research Output API records
  DIR_PURE_RSOUT = "/pure/research-output-api/rmids/prd_batch_all"
  FILE_PREFIX_PURE_RSOUT = "pureIdsByRmid_"
  FILE_EXT_PURE_RSOUT = "xml"

  # DSpace parameters
  DSPACE_ASSETSTORE_DIRPATH = "/file/path/to/dspace/assetstore/"
  DSPACE_ASSETSTORE_BASE_URL = "https://example.com/dspace-assetstore/"

  HANDLE_URL_LEFT_STRING = 'https://dspace.example.com/xmlui/handle/'

  DSPACE_EXE = "/file/path/to/dspace/bin/dspace"
  DSPACE_EPERSON4PACKAGER = "admin_user@example.com"	# Email address of the E-Person under whose authority this runs
  DSPACE_PKG_CMD_PART1 = "#{DSPACE_EXE} packager --disseminate --type AIP --option manifestOnly=true --eperson #{DSPACE_EPERSON4PACKAGER}"

  # XPath to various Dublin Core elements within DSpace AIP package
  XPATH_DC_PREFIX = "/mets/dmdSec/mdWrap/xmlData/dim:dim/dim:field"
  XPATH_DC = {
    :description	=> "#{XPATH_DC_PREFIX}[@element='description'][not(@qualifier)]",
    :doi		=> "#{XPATH_DC_PREFIX}[@element='identifier'][@qualifier='doi']",
    :rmid		=> "#{XPATH_DC_PREFIX}[@element='identifier'][@qualifier='rmid']",
    :publisher		=> "#{XPATH_DC_PREFIX}[@element='publisher'][not(@qualifier)]",
    :rights		=> "#{XPATH_DC_PREFIX}[@element='rights'][not(@qualifier)]",
    :license		=> "#{XPATH_DC_PREFIX}[@element='rights'][@qualifier='license']",
    :relation		=> "#{XPATH_DC_PREFIX}[@element='relation'][not(@qualifier)]",
    :grantnumber	=> "#{XPATH_DC_PREFIX}[@element='relation'][@qualifier='grantnumber']",
    :title		=> "#{XPATH_DC_PREFIX}[@element='title'][not(@qualifier)]",
  }

  VERBOSE_DOCVERSIONS = {
    "author"	=> "AuthorAcceptedManuscript",
    "publisher"	=> "FinalPublishedVersion",
    "unknown"	=> "OtherVersion",
  }

  # Regular expressions for detecting various parameters
  OPEN_ACCESS_HOST_REGEX = /^fac($|\.)/

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

  LICENCE_ABBR_TARGETS = [
    "CC0",
    "CC-BY",
    "CC-BY-NC",
    "CC-BY-NC-ND",
    "CC-BY-NC-SA",
    "CC-BY-ND",
    "CC-BY-SA",
    "In Copyright",
  ]

  # Regexs for matching licence URLs
  # - Permit zero or more white-space chars (including newline) on either side of "/"
  LICENCE_URL_MID = "creativecommons.org/licenses/"
  LICENCE_URL_K_V_LIST = LICENCE_KEYS.map{|k|
    ks = k.to_s
    v = "#{LICENCE_URL_MID}#{ks[3...ks.size].gsub("_", "-")}/".gsub("/", '\s*/\s*')
    [k, Regexp.new(v, Regexp::IGNORECASE)]
  }
  LICENCE_URL_REGEX_LIST = Hash[LICENCE_URL_K_V_LIST]

  DOI_DEL_URL_REGEX = /^.*doi\.org\//i

  # XPath (excluding root element) to various Pure Research Output API XML elements
  XPATH_RSOUT_DOI	= "electronicVersions/electronicVersion/doi"
  XPATH_RSOUT_EXT_ID	= "info/additionalExternalIds/id"
  XPATH_RSOUT_UUID	= "info/previousUuids/previousUuid"
  XPATH_RSOUT_PORTAL	= "info/portalUrl"

  # FasterCSV options for writing CSV to output
  FCSV_OUT_OPTS = {
    :col_sep => ',',
    :headers => true,
    :force_quotes => true,
  }
end

