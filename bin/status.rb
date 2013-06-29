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

    def table_count_by_grouping table, group_col
      result = @conn.exec("
        select #{group_col.to_s}, count(*) as c
        from #{table.to_s}
        group by #{group_col.to_s}")
      {false => result[0]['c'].to_i, true => result[1]['c'].to_i}
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
      queue_table_status :kgs_usernames, "Usernames"
      queue_table_status :kgs_month_urls, "Month URLs"
    end

    private

    # `pct` calls `to_f` to avoid rescuing `ZeroDivisionError`
    def pct n, total
      (n.to_f / total.to_f) * 100.0
    end

    def queue_table_status table, description
      hsh = @db.table_count_by_grouping table, :requested
      puts "%10d %s requested" % [hsh[true], description]
      puts "%10d %s pending" % [hsh[false], description]
      queue_pct = pct(hsh[true], hsh[true] + hsh[false])
      puts "%10.2f %s pct req." % [queue_pct, description]
    end
  end
end

GagraStatus::Main.new.run
