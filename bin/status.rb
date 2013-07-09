#!/usr/bin/env ruby
$stdout.sync = true

require 'action_view/helpers/number_helper'
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

    def table_count_by_grouping table, group_col
      result = @conn.exec("
        select #{group_col.to_s}, count(*) as c
        from #{table.to_s}
        group by #{group_col.to_s}")
      {false => result[0]['c'].to_i, true => result[1]['c'].to_i}
    end

    def usernames_without_games
      result = @conn.exec("
        select count(u.*) as c
        from kgs_usernames u
        left join (select distinct lower(kgs_un_w) as un from games) w
          on w.un = lower(u.un)
        left join (select distinct lower(kgs_un_b) as un from games) b
          on b.un = lower(u.un)
        where w.un is null
          and b.un is null;")
      result[0]['c'].to_i
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
    include ActionView::Helpers::NumberHelper

    NUM_COL_WIDTH = 11.freeze

    def initialize
      @db = Db.new
    end

    def run
      puts "%#{NUM_COL_WIDTH}s   %s" % [intf(@db.game_count), 'Games']
      queue_table_status :kgs_usernames, "Usernames"
      queue_table_status :kgs_month_urls, "Month URLs"
      puts "%#{NUM_COL_WIDTH}s   %s" % [intf(@db.usernames_without_games), 'Usernames without games']
    end

    private

    # Using space as a delimiter is recommended in the SI/ISO 31-0 standard
    # http://en.wikipedia.org/wiki/ISO_31-0#Numbers
    def intf i
      number_with_delimiter i, :delimiter => ' '
    end

    # `pct` calls `to_f` to avoid rescuing `ZeroDivisionError`
    def pct n, total
      (n.to_f / total.to_f) * 100.0
    end

    def queue_table_status table, description
      hsh = @db.table_count_by_grouping table, :requested
      puts "%#{NUM_COL_WIDTH}s   %s requested" % [intf(hsh[true]), description]
      puts "%#{NUM_COL_WIDTH}s   %s pending" % [intf(hsh[false]), description]
      queue_pct = pct(hsh[true], hsh[true] + hsh[false])
      puts "%#{NUM_COL_WIDTH+2}.1f %s pct req." % [queue_pct, description]
    end
  end
end

GagraStatus::Main.new.run
