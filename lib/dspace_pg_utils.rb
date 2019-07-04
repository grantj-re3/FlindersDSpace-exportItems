#--
# Copyright (c) 2014-2019, Flinders University, South Australia. All rights reserved.
# Contributors: Library, Corporate Services, Flinders University.
# See the accompanying LICENSE file (or http://opensource.org/licenses/BSD-3-Clause).
#++

require 'rubygems'
require 'pg'
require 'pg_extra'
require 'dbc'
require 'dspace_utils'

##############################################################################
# Handy DSpace PostgreSQL utilities and constants
#
# DSpace constants, methods, etc which might be used without making a
# database connection *must* be put into dspace_utils.rb. Database
# related DSpace functionality should be put into this module.
##############################################################################
module DSpacePgUtils
  include DbConnection
  include DSpaceUtils

  # This hash shows the relationship between the DSpace handle table's
  # resource_type_id and its type. ie. RESOURCE_TYPE_IDS[type] = resource_type_id
  RESOURCE_TYPE_IDS = {
    :bitstream	=> 0,
    :bundle	=> 1,
    :item	=> 2,
    :collection	=> 3,
    :community	=> 4,
  }

  # This table shows the inverse. ie. RESOURCE_TYPES[resource_type_id] = type
  RESOURCE_TYPES = RESOURCE_TYPE_IDS.invert

  # This hash shows the relationship between the DSpace resourcepolicy table's
  # action_id and its action. ie. POLICY_ACTION_IDS[action] = action_id
  POLICY_ACTION_IDS = {
    :read			=> 0,
    :write			=> 1,
    :delete			=> 2,
    :add			=> 3,
    :remove			=> 4,

    :workflow_step_1		=> 5,
    :workflow_step_2		=> 6,
    :workflow_step_3		=> 7,
    :workflow_abort		=> 8,

    :default_bitstream_read	=> 9,
    :default_item_read		=> 10,
    :admin			=> 11,
  }

  # This table shows the inverse. ie. POLICY_ACTION[action_id] = action
  POLICY_ACTION = POLICY_ACTION_IDS.invert

  private

  ############################################################################
  # Yield a connection to the DSpace database. If @db_conn is nil we
  # will open and yield a new connection. Otherwise we assume that
  # @db_conn is a valid connection and we will yield it.
  ############################################################################
  def db_connect
    conn = @db_conn ? @db_conn : PG::Connection.connect2(DB_CONNECT_INFO)
    yield conn
  end

end

