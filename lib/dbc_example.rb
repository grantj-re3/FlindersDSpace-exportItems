# For more information see:
# https://github.com/grantj-re3/FlindersDSpace-importERA/blob/master/README_hdl2item_bmecsv.md#prerequisites

module DbConnection

  # For the 'pg' library
  DB_CONNECT_INFO = {
    :dbname => "my_dspace_database_name",
    :user => "my_db_username",

    # If applicable, configure password, remote host, remote port, etc.
    #:password => "my_db_password",
    #:host => "my_db_host",
    #:port => "my_db_port",
  }

end

