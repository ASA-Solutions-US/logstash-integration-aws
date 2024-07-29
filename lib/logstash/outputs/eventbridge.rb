# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "logstash/util"
require "logstash/util/unicode_trimmer"

# EventBridge output.
#
# Send events to Amazon EventBridge, a serverless event bus service that
# makes it easy to connect application data from various sources.
#
# For further documentation about the service see:
#
#   https://docs.aws.amazon.com/eventbridge/latest/userguide/what-is-amazon-eventbridge.html
#
# This plugin looks for the following fields on events it receives:
#
#  * `eventbridge` - If no event bus name is found in the configuration file, this will be used as
#  the event bus name to publish to.
#  * `eventbridge_source` - The source of the event.
#  * `eventbridge_detail_type` - The detail type of the event.
#  * `eventbridge_detail` - The detail of the event, must be a valid JSON string.
#
class LogStash::Outputs::EventBridge < LogStash::Outputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  config_name "eventbridge"
  
  concurrency :shared

  # Optional Event Bus name to send events to. If you do not set this you must
  # include the `eventbridge` field in your events to set the event bus name on a per-event basis!
  config :event_bus_name, :validate => :string

  # When an event bus name is specified here, a "Logstash successfully booted" event will be sent to it when this plugin
  # is registered.
  #
  # Example: default
  #
  config :publish_boot_event_bus_name, :validate => :string

  public
  def register
    require "aws-sdk-eventbridge"

    @eventbridge = Aws::EventBridge::Client.new(aws_options_hash)

    publish_boot_event()

    @codec.on_event do |event, encoded|
      send_eventbridge_event(event_bus_name(event), event_source(event), event_detail_type(event), encoded)
    end
  end

  public
  def receive(event)
    if (event_detail = event.get("eventbridge_detail"))
      if event_detail.is_a?(String)
        send_eventbridge_event(event_bus_name(event), event_source(event), event_detail_type(event), event_detail)
      else
        @codec.encode(event_detail)
      end
    else
      @codec.encode(event)
    end
  end

  private
  def publish_boot_event
    # Try to publish a "Logstash booted" event to the EventBus provided to
    # cause an error ASAP if the credentials are bad.
    if @publish_boot_event_bus_name
      send_eventbridge_event(@publish_boot_event_bus_name, 'Logstash', 'LogstashBoot', 'Logstash successfully booted')
    end
  end

  private
  def send_eventbridge_event(event_bus_name, source, detail_type, detail)
    raise ArgumentError, 'An EventBridge EventBus name is required.' unless event_bus_name

    @logger.debug? && @logger.debug("Sending event to EventBridge EventBus [#{event_bus_name}] with source [#{source}], detail type [#{detail_type}] and detail: #{detail}")

    @eventbridge.put_events({
      entries: [
        {
          event_bus_name: event_bus_name,
          source: source,
          detail_type: detail_type,
          detail: detail
        }
      ]
    })
  end

  private
  def event_bus_name(event)
    event.get("eventbridge") || @event_bus_name
  end

  private
  def event_source(event)
    event.get("eventbridge_source") || 'logstash'
  end

  private
  def event_detail_type(event)
    event.get("eventbridge_detail_type") || 'logstashEvent'
  end
end
