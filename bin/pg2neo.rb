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
      @uname_to_node = {}
    end

    def run
      neo_delete_all
      create_nodes_from_usernames
      create_relationships_from_games
    end

    private

    def create_nodes_from_usernames
      usernames.each_with_index do |un, ix|
        n = @neo.create_node("kgsun" => un.to_s)
        @uname_to_node[un] = n
        puts sprintf 'node: %s %d', un, nid(n)
      end
    end

    def create_relationships_from_games
      games.each_with_index do |g, ix|
        unw = g['kgs_un_w']
        unb = g['kgs_un_b']
        puts sprintf 'rel: w: %s %d b: %s %d', \
          unw, nid(@uname_to_node[unw]), unb, nid(@uname_to_node[unb])
        w = @uname_to_node[unw]
        b = @uname_to_node[unb]
        rel = @neo.create_relationship("game", w, b)
        props = g.to_hash.reject { |k,v| ['kgs_un_w', 'kgs_un_b'].include? k }
        @neo.set_relationship_properties(rel, props)
      end
    end

    def games
      tuples = @pg.exec "
        select *
        from games
        where kgs_un_w in (#{username_qry})
          and kgs_un_b in (#{username_qry})
        limit 100"
      puts sprintf 'pg: %d games', tuples.ntuples
      return tuples
    end

    def neography_client
      Neography.configure do |config|
        config.server         = "localhost"
        config.port           = 7474
      end
      Neography::Rest.new
    end

    def nid n
      n['self'].split('/').last
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
      tuples = @pg.exec username_qry
      puts sprintf 'pg: %d usernames', tuples.ntuples
      return tuples.field_values('un')
    end

    def username_qry
      'select un from kgs_usernames where requested = true limit 1000'
    end

  end
end

Pg2Neo::Main.new.run
