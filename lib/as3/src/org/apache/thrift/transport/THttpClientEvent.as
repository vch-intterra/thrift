package org.apache.thrift.transport {
	import flash.events.ErrorEvent;

	/**
	 * @author ivaskov
	 */
	public class THttpClientEvent extends ErrorEvent {
		
		public var error:Error;
		
		public function THttpClientEvent(type : String, error:Error) {
			super(type, false, false, error.toString());
			this.error = error;
		}
	}
}
