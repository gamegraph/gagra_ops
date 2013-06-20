#!/usr/bin/env ruby

require 'dotenv'
require 'uri'

Dotenv.load

uri = URI.parse ENV.fetch 'DATABASE_URL'
db = uri.path[1..-1]
ENV['PGPASSWORD'] = uri.password
exec "psql -h #{uri.host} -U #{uri.user} #{db}"
