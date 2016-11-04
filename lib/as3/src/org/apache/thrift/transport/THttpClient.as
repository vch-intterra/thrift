/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift.transport {
	import flash.errors.EOFError;
	import flash.events.Event;
	import flash.events.IOErrorEvent;
	import flash.events.SecurityErrorEvent;
	import flash.net.URLLoader;
	import flash.net.URLLoaderDataFormat;
	import flash.net.URLRequest;
	import flash.net.URLRequestMethod;
	import flash.utils.ByteArray;

	/**
	 * HTTP implementation of the TTransport interface. Used for working with a
	 * Thrift web services implementation.
	 */
	public class THttpClient extends TTransport {

		private var request_ : URLRequest = null;
		private var requestBuffer_ : ByteArray = new ByteArray();
		private var responseBuffer_ : ByteArray = null;

		public function getBuffer() : ByteArray {
			return requestBuffer_;
		}

		public function THttpClient(request : URLRequest) : void {
			request_ = request;
		}

		public override function open() : void {
		}

		public override function close() : void {
		}

		public override function isOpen() : Boolean {
			return true;
		}

		public override function read(buf : ByteArray, off : int, len : int) : int {
			if (responseBuffer_ == null) {
				throw new TTransportError(TTransportError.UNKNOWN, "Response buffer is empty, no request.");
			}
			try {
				responseBuffer_.readBytes(buf, off, len);
				return len;
			}
			catch (e : EOFError) {
				throw new TTransportError(TTransportError.UNKNOWN, "No more data available.");
			}
			return 0;
		}

		public override function write(buf : ByteArray, off : int, len : int) : void {
			requestBuffer_.writeBytes(buf, off, len);
		}

		public override function flush(seqId:int,callback : Function = null) : void {
			var loader : URLLoader = new URLLoader();
			
      
			if (callback != null) {
				loader.addEventListener(Event.COMPLETE, function(event : Event):void {
        
					responseBuffer_ = URLLoader(event.target).data;
 
					try {
						callback();
					}catch (e:Error){
						dispatchEvent(new THttpClientEvent(e.name, e));
					}
					responseBuffer_ = null;
				}); 
			}
			loader.addEventListener(IOErrorEvent.IO_ERROR, onIOError, false, 0, true);
			loader.addEventListener(IOErrorEvent.NETWORK_ERROR, onNetworkError, false, 0, true);
			loader.addEventListener(SecurityErrorEvent.SECURITY_ERROR, onSecurityError, false, 0, true);
			request_.method = URLRequestMethod.POST;
			loader.dataFormat = URLLoaderDataFormat.BINARY;
			requestBuffer_.position = 0;
			request_.data = requestBuffer_;
			loader.load(request_);
		}
		
		private function onSecurityError(event : SecurityErrorEvent) : void {
			dispatchEvent(event.clone());
		}

		private function onIOError(event : IOErrorEvent) : void {
			dispatchEvent(event.clone());
		} 
		
		private function onNetworkError(event : IOErrorEvent) : void {
			dispatchEvent(event.clone());
		}
		
	}
}
