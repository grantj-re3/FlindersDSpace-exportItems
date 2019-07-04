#--
# Copyright (c) 2014, Flinders University, South Australia. All rights reserved.
# Contributors: eResearch@Flinders, Library, Information Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#++

##############################################################################
# Handy DSpace utilities and constants
#
# Constants, methods, etc within this module *must* be accessible without
# making a database connection. Database related DSpace functionality
# should be put into dspace_pg_utils.rb.
##############################################################################
module DSpaceUtils
  # In a single CSV column, use this delimiter to separate multiple values
  VALUE_DELIMITER = '||'

  # Newline for your platform (eg. for XML or CSV output)
  NEWLINE = "\n"
end

