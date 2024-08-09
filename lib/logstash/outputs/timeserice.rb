# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "logstash/util"
require "logstash/util/unicode_trimmer"

# Timestream output.
#
# Send events to Amazon Timestream, a serverless time series database service.
# 
# For further documentation about the service see:
#
#   https://docs.aws.amazon.com/timestream/latest/developerguide/what-is-timestream.html
#
class LogStash::Outputs::Timestream < LogStash::Outputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  config_name "timestream"

  concurrency :shared

  # The Timestream database name.
  config :database_name, :validate => :string, :required => true

  # The Timestream table name.
  config :table_name, :validate => :string, :required => true

  # The dimensions to include in the Timestream records.
  # This should be an array of hashes where each hash represents a dimension with `name` and `value`.
  config :dimensions, :validate => :array, :default => []

  # The time unit of the timestamp field. Options: SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS
  config :time_unit, :validate => :string, :default => "MILLISECONDS"

  public
  def register
    require "aws-sdk-timestreamwrite"

    @timestream = Aws::TimestreamWrite::Client.new(aws_options_hash)
  end

  public
  def receive(event)
    begin
      records = build_records(event)
      @timestream.write_records({
        database_name: @database_name,
        table_name: @table_name,
        records: records
      })
      @logger.info("Event successfully sent to Timestream")
    rescue => e
      @logger.error("Failed to send event to Timestream", :exception => e, :event => event)
    end
  end

  private
  def build_records(event)
    dimensions = @dimensions.map do |dim|
      {
        name: dim["name"],
        value: event.get(dim["value"])
      }
    end

    [{
      dimensions: dimensions,
      measure_name: event.get("measure_name") || "default_measure",
      measure_value: event.get("measure_value"),
      measure_value_type: event.get("measure_value_type") || "DOUBLE",
      time: event.get("timestamp") || Time.now.to_i.to_s,
      time_unit: @time_unit
    }]
  end
end
