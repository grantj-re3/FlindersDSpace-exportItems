#
# Copyright (c) 2019, Flinders University, South Australia. All rights reserved.
# Contributors: Library, Corporate Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#
# Config vars for ruby

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

  DIR_TOP = File.expand_path("..", File.dirname(__FILE__))
  DIR_RESULTS = "#{DIR_TOP}/results"

  DIR_DSPACE_PACKAGE = "#{DIR_TOP}/results/aip"
  BASENAME_DSPACE_PACKAGE = "aip_"

  DIR_DSPACE_OUT = "#{DIR_TOP}/results/out"
  BASENAME_DSPACE_OUT = "out_"

  DSPACE_ASSETSTORE_DIRPATH = "/file/path/to/dspace/assetstore/"

  HANDLE_URL_LEFT_STRING = 'https://dspace.example.com/xmlui/handle/'

  DSPACE_EXE = "/file/path/to/dspace/bin/dspace"
  DSPACE_EPERSON4PACKAGER = "admin_user@example.com"	# Email address of the E-Person under whose authority this runs
  DSPACE_PKG_CMD_PART1 = "#{DSPACE_EXE} packager --disseminate --type AIP --option manifestOnly=true --eperson #{DSPACE_EPERSON4PACKAGER}"

  MAX_ITEMS_TO_PROCESS = 1000	# FIXME: Currently unused
end

