#!/usr/bin/env ruby

require 'dotenv'
require 'pg'
require 'pp'
require 'uri'

Dotenv.load

module GagraStatus
  class Db
    def initialize
      @conn = PG.connect connect_hash db_uri
    end

    def game_count
      table_count 'games'
    end

    def player_count
      table_count 'players'
    end

    def username_count_by_requested
      result = @conn.exec("
        select requested, count(*) as c
        from kgs_usernames
        group by requested")
      {requested: result[1]['c'], not_requested: result[0]['c']}
    end

    private

    def connect_hash uri
      {
        host: uri.host,
        dbname: uri.path[1..-1],
        user: uri.user,
        password: uri.password
      }
    end

    def db_uri
      URI.parse ENV['DATABASE_URL']
    end

    def table_count table
      @conn.exec("select count(*) as c from #{table}")[0]['c']
    end
  end

  class Main
    def initialize
      @db = Db.new
    end

    def run
      puts "%10d %s" % [@db.player_count, 'Players']
      puts "%10d %s" % [@db.game_count, 'Games']
      unc = @db.username_count_by_requested
      puts "%10d %s" % [unc[:requested].to_i, 'Usernames requested']
      puts "%10d %s" % [unc[:not_requested].to_i, 'Usernames pending']
      total_un = unc[:requested].to_i + unc[:not_requested].to_i
      puts "%10.2f %s" % [pct(unc[:requested], total_un), 'Usernames pct req.']
    end

    private

    # `pct` calls `to_f` to avoid rescuing `ZeroDivisionError`
    def pct n, total
      (n.to_f / total.to_f) * 100.0
    end

  end
end

GagraStatus::Main.new.run
