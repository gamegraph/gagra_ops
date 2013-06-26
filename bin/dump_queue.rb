#!/usr/bin/env ruby

require 'aws/sqs'
require 'dotenv'
require 'uri'

if ARGV.length != 1
  $stderr.puts "Usage: bin/dump_queue.rb queue_name > filename"
  exit 1
end

class DumpQueue
  def initialize queue_name
    Dotenv.load
    sqs = AWS::SQS.new aws_cred
    @q = sqs.queues.named queue_name
  end

  def run
    $stdout.sync = true # Don't buffer stdout
    naptime = 5.0 # initial backoff
    while naptime < 300 do
      begin
        @q.poll(poll_opts) do |msg|
          naptime = 5.0 # success, so reset backoff
          puts msg.body
        end
      rescue SocketError
        $stderr.puts "SocketError: " + $!.to_s
      end
      sleep naptime
      naptime = naptime ** 1.5 # exponential backoff
    end
  end

  def aws_cred
    {
      access_key_id: ENV.fetch('AWS_ACCESS_KEY_ID'),
      secret_access_key: ENV.fetch('AWS_SECRET_ACCESS_KEY')
    }
  end

  def poll_opts
    {:batch_size => 10, :idle_timeout => 3}
  end
end

DumpQueue.new(ARGV[0]).run
