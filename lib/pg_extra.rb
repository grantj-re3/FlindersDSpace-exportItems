#--
# Copyright (c) 2014, Flinders University, South Australia. All rights reserved.
# Contributors: eResearch@Flinders, Library, Information Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#++ 

require 'rubygems'
require 'pg'

##############################################################################
# Extend the PG::Connection class (which is the connection component of
# the PostgreSQL database driver).
class PG::Connection

  ############################################################################
  # Has the same function as the connect method except the connection
  # object can be yielded to a block. Usage example is:
  #   PG::Connection.connect2(connect_args){|conn| ... }
  def self.connect2(args)
    conn = self.new(args)
    return conn unless block_given?

    begin
      yield conn
    ensure
      conn.close
    end
  end

end

