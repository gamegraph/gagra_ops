#!/usr/bin/env ruby
$stdout.sync = true

require 'dotenv'
require 'rubygems'
require 'neography'
require 'pg'
require 'pry'
require 'uri'

module Pg2Neo
  class Main
    BATCH_SIZE = 1000
    NEO4J_DATA_DIR = '/usr/local/Cellar/neo4j/community-1.9.1-unix/libexec/data'

    def initialize
      Dotenv.load
      @pg = pg_client
      @neo = neography_client
      @uname_to_nodeid = {}
    end

    def run
      neo_delete_all
      create_nodes_from_usernames
      create_relationships_from_games
    end

    private

    def create_nodes_from_usernames
      puts "Creating nodes .."
      usernames.each_slice(BATCH_SIZE) do |batch|
        batch_cmds = batch.map { |un| [:create_node, node(un)] }
        responses = @neo.batch *batch_cmds
        responses.each do |r|
          node_id = r.fetch("location").split("/").last.to_i
          un = r.fetch("body").fetch("data").fetch("kgsun").to_s.downcase
          @uname_to_nodeid[un] = node_id
        end
        print '.'
      end
      puts ' '
    end

    def create_relationships_from_games
      puts "Creating relationships .."
      games.each_slice(BATCH_SIZE) do |batch|
        batch_cmds = []
        batch.each do |game|
          unw = game['kgs_un_w'].downcase
          unb = game['kgs_un_b'].downcase
          w = @uname_to_nodeid[unw]
          b = @uname_to_nodeid[unb]
          if !w.nil? && !b.nil?
            props = game.to_hash.reject { |k,v| ['kgs_un_w', 'kgs_un_b'].include? k }
            batch_cmds << [:create_relationship, "game", w, b, props]
          else
            print 's' # "skipped"
          end
        end
        @neo.batch *batch_cmds
        print '.'
      end
      puts ' '
    end

    def games
      tuples = @pg.exec "select * from games"
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

    def node username
      {"kgsun" => username.to_s}
    end

    def pg_client
      uri = URI.parse ENV['DATABASE_URL']
      PG.connect(
        host: uri.host,
        dbname: uri.path[1..-1],
        user: uri.user,
        password: uri.password)
    end

    # Deleting millions of nodes and relationships in `neo4j` is very slow
    # using a query like `start a=node(*) match a-[r?]-() delete a,r`.
    # However, it seems to be an accepted practice to simply delete
    # the `graph.db` file.
    def neo_delete_all
      puts "Stopping neo4j .."
      system "neo4j stop"
      puts "Deleting graph.db .."
      system "rm -r #{NEO4J_DATA_DIR}/graph.db"
      puts "Starting neo4j .."
      system "neo4j start"
    end

    def usernames
      tuples = @pg.exec 'select un from kgs_usernames'
      puts sprintf 'pg: %d usernames', tuples.ntuples
      return tuples.field_values('un')
    end
  end
end

Pg2Neo::Main.new.run
