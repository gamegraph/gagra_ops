#!/usr/bin/env ruby

require 'dotenv'
require 'uri'

Dotenv.load

DEST="/Users/jared/git/gamegraph/dumps"

uri = URI.parse ENV.fetch 'DATABASE_URL'
db = uri.path[1..-1]
ENV['PGPASSWORD'] = uri.password
fn = File.join(DEST, Time.now.strftime('%Y%m%d') + ".dump")
exec "pg_dump -h #{uri.host} -U #{uri.user} -Fc -v -f #{fn} #{db}"
