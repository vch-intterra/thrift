package org.apache.thrift.transport
{
	import flash.events.Event;
	
	public class TFlashSocketClientEvent extends Event
	{
		
			
		public static const ERROR_CONNECT:String = "ERROR_CONNECT";
		
		public function TFlashSocketClientEvent(type:String, bubbles:Boolean=false, cancelable:Boolean=false)
		{
			super(type, bubbles, cancelable);
		}
	}
}