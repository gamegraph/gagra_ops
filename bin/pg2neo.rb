#!/usr/bin/env ruby

require 'dotenv'
require 'rubygems'
require 'neography'
require 'pg'
require 'uri'

module Pg2Neo
  class Main
    def initialize
      Dotenv.load
      @pg = pg_client
      @neo = neography_client
    end

    def run
      neo_delete_all
      usernames.each_with_index do |un, ix|
        @neo.create_node("kgsun" => un.to_s)
        puts ix if ix % 100 == 0
      end
    end

    private

    def neography_client
      Neography.configure do |config|
        config.server         = "localhost"
        config.port           = 7474
      end
      Neography::Rest.new
    end

    def pg_client
      uri = URI.parse ENV['DATABASE_URL']
      PG.connect(
        host: uri.host,
        dbname: uri.path[1..-1],
        user: uri.user,
        password: uri.password)
    end

    def neo_delete_all
      @neo.execute_query("start a=node(*) match a-[r?]-() delete a,r")
    end

    def usernames
      tuples = @pg.exec 'select un from kgs_usernames where requested = true'
      puts sprintf 'pg: %d usernames', tuples.ntuples
      return tuples.field_values('un')
    end

  end
end

Pg2Neo::Main.new.run
