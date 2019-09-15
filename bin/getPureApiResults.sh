#!/bin/sh
#
# Usage:  ./getPureApiResults.sh   FNAME_WITH_SRC_IDS
#
# Copyright (c) 2019, Flinders University, South Australia. All rights reserved.
# Contributors: Library, Corporate Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#
# PURPOSE
#
# The purpose of this program is to extract miscellaneous info (such as
# identifiers) about Pure records (such as research-outputs). It assumes
# you have a list of (source) IDs in file FNAME_WITH_SRC_IDS (one per
# line).
#
# API documentation should be available on your Pure server at a URL like:
#   https://my-pure-server/ws/api/514/api-docs/index.html
#
# GOTCHAS
# - Requires curl & xmllint.
# - When running this (rough) script, you should be in the parent directory
#   of the location where the following folders will be created:
#   * xml:	stores direct results of API calls
#   * fmtxml:	stores pretty (indented) results of API calls
#
##############################################################################
api_key="my-api-key"					# CUSTOMISE
api_version="514"					# CUSTOMISE

# CUSTOMISE
url_prefix="https://my-pure-server/ws/api/$api_version/research-outputs"

# CUSTOMISE
fields=`cat <<-EO_LIST |tr -d '\r\n' |sed 's/,/%2C/g'
	title,
	personAssociations.personAssociation,
	electronicVersions.electronicVersion.*,
	info.*
EO_LIST
`

##############################################################################
fname_csv_in="$1"
[ ! -f "$fname_csv_in" ] && {
  echo "Quitting: File not found: '$fname_csv_in'"
  exit 1
}

mkdir xml fmtxml 2>/dev/null			# FIXME: Relative path

while read rmid; do
  fname_xml_out="xml/pureIdsByRmid_$rmid.xml"	# FIXME: Relative path
  fname_fmt_out="fmt$fname_xml_out"		# FIXME: Relative path

  echo
  echo "`date '+%F %T'` Writing file: $fname_fmt_out (via $fname_xml_out)"
  cmd="curl -sSk -o '$fname_xml_out' -X GET --header 'Accept: application/xml' --header 'api-key: $api_key' '$url_prefix/$rmid?idClassification=source&fields=$fields'"
  echo "CMD: $cmd"

  eval $cmd
  ret=$?
  if [ $ret = 0 ]; then
    xmllint --format "$fname_xml_out" > "$fname_fmt_out"

    if egrep -q "</error>" "$fname_fmt_out"; then
      echo "API ERROR found in '$fname_fmt_out'"
    fi

  else
    echo "ERROR: curl return code '$ret'. Not writing to $fname_fmt_out"
  fi
done < "$fname_csv_in"

