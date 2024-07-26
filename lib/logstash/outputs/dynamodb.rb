# lib/logstash/outputs/dynamodb.rb
require "logstash/outputs/base"
require "logstash/namespace"
require "aws-sdk-dynamodb"

class LogStash::Outputs::Dynamodb < LogStash::Outputs::Base
  config_name "dynamodb"

  # Define the configuration parameters
  config :table_name, :validate => :string, :required => true
  config :region, :validate => :string, :default => "us-east-1"
  config :access_key_id, :validate => :string, :required => true
  config :secret_access_key, :validate => :string, :required => true

  public
  def register
    @client = Aws::DynamoDB::Client.new(
      region: @region,
      access_key_id: @access_key_id,
      secret_access_key: @secret_access_key
    )
  end

  public
  def receive(event)
    item = event.to_hash
    params = {
      table_name: @table_name,
      item: item
    }

    @client.put_item(params)
  rescue => e
    @logger.error("Failed to put item to DynamoDB", :exception => e, :event => event)
  end
end
