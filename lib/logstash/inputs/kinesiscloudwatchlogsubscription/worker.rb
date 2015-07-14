class LogStash::Inputs::KinesisCloudWatchLogSubscription::Worker
  include com.amazonaws.services.kinesis.clientlibrary.interfaces::IRecordProcessor

  attr_reader(
    :checkpoint_interval,
    :codec,
    :decorator,
    :logger,
    :output_queue,
  )

  def initialize(*args)
    # nasty hack, because this is the name of a method on IRecordProcessor, but also ruby's constructor
    if !@constructed
      @codec, @output_queue, @decorator, @checkpoint_interval, @logger = args
      @next_checkpoint = Time.now - 600
      @constructed = true
    else
      _shard_id, _ = args
      @decoder = java.nio.charset::Charset.forName("UTF-8").newDecoder()
    end
  end
  public :initialize

  def processRecords(records, checkpointer)
    records.each { |record| process_record(record) }
    if Time.now >= @next_checkpoint
      checkpoint(checkpointer)
      @next_checkpoint = Time.now + @checkpoint_interval
    end
  end

  def shutdown(checkpointer, reason)
    if reason == com.amazonaws.services.kinesis.clientlibrary.types::ShutdownReason::TERMINATE
      checkpoint(checkpointer)
    end
  end

  protected

  def checkpoint(checkpointer)
    checkpointer.checkpoint()
  rescue => error
    @logger.error("Kinesis worker failed checkpointing: #{error}")
  end

	def process_record(record)
		# I'm SOOOO SORRY. This is fugly. But it works. And lets me ship.
		# Please make this right. I was always getting incorrect header errors.
		# Either with JRuby zlib, or raw java zlib :(
		raw = record.getData
		File.open( '/tmp/sequence-' + record.sequenceNumber, 'w+') do |file|
			file.write(raw.array)
		end

		raw = `zcat /tmp/sequence-#{record.sequenceNumber}`
		File.delete "/tmp/sequence-#{record.sequenceNumber}"

		@codec.decode(raw) do |event|
			@decorator.call(event)
			@output_queue << event
		end
	rescue => error
		@logger.error("Error processing record: #{error}")
	end
end
