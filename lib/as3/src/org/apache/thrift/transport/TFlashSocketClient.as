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

package org.apache.thrift.transport
{
    
    import flash.errors.EOFError;
    import flash.events.Event;
    import flash.events.EventDispatcher;
    import flash.events.IOErrorEvent;
    import flash.events.ProgressEvent;
    import flash.events.SecurityErrorEvent;
    import flash.net.Socket;
    import flash.net.URLLoader;
    import flash.net.URLLoaderDataFormat;
    import flash.net.URLRequest;
    import flash.net.URLRequestMethod;
    import flash.utils.ByteArray;
    import flash.utils.IDataInput;
    import flash.utils.IDataOutput;
    import flash.utils.clearInterval;
    import flash.utils.setInterval;
    import flash.utils.setTimeout;


    /**
     * HTTP implementation of the TTransport interface. Used for working with a
     * Thrift web services implementation.
     * Unlike Http Client, it uses a single POST, and chunk-encoding to transfer all messages.
     */

    public class TFlashSocketClient extends TTransport
    {
        private var socket:Socket = null;

        private var host:String;

        private var port:int;

        private var obuffer:ByteArray = new ByteArray();
		
		private var seqId:int = 1;

        private var bytesInChunk:int = 0;

        private var callbacks:Object = new Object ();
		
		private var responseBuffer:ByteArray = new ByteArray ();

       	private var connectCoun:int = 0;
		
		public var isPossibleConnected:Boolean;
		
		private var si:int = -1;
		
		
		public function get connected ():Boolean {
			return socket.connected;
		}
		

        public function TFlashSocketClient(host:String, port:int):void
        {
            this.host = host;
            this.port = port;
        }

        public override function close():void
        {
			callbacks = new Object ();
			
			if (socket.connected){
            	socket.close();
			}
			
			if (si != -1){
				clearInterval(si);
				si = -1;
			}
        }

    	public override function peek():Boolean
    	{
			if(socket.connected)
			{
				trace("Bytes remained:" + socket.bytesAvailable);
				return socket.bytesAvailable>0;
			}
			return false;
		}

        public override function read(buf:ByteArray, off:int, len:int):int
        {
			if (responseBuffer == null) {
				throw new TTransportError(TTransportError.UNKNOWN, "Response buffer is empty, no request.");
			}
			try {
				responseBuffer.readBytes(buf, off, len);
				return len;
			}
			catch (e : EOFError) {
				throw new TTransportError(TTransportError.UNKNOWN, "No more data available.");
			}
            return 0;
        }

        public function debugBuffer(buf:ByteArray):void
        {
            var debug:String = "BUFFER >>";
            var i:int;
            for (i = 0; i < buf.length; i++)
            {
                debug += buf[i] as int;
                debug += " ";
            }

            trace(debug + "<<");
        }

        public override function write(buf:ByteArray, off:int, len:int):void
        {
            obuffer.writeBytes(buf, off, len);
        }

        public override function open():void
        {
			
			addEventListener(IOErrorEvent.IO_ERROR, socketErrorEmty);
			addEventListener(SecurityErrorEvent.SECURITY_ERROR, socketSecurityErrorEmty);
			
            this.socket = new Socket();
            this.socket.addEventListener(Event.CONNECT, socketConnected);
            this.socket.addEventListener(IOErrorEvent.IO_ERROR, socketError);
            this.socket.addEventListener(SecurityErrorEvent.SECURITY_ERROR, socketSecurityError);
			this.socket.addEventListener(Event.CLOSE, sockeCloseHandler);
            this.socket.addEventListener(ProgressEvent.SOCKET_DATA, socketDataHandler);
			 
            this.socket.connect(host, port);
        }
		
		private function socketErrorEmty (event:IOErrorEvent):void {
			
		}
		
		private function socketSecurityErrorEmty (event:SecurityErrorEvent):void {
			
		}
		

		
		public function reconnect ():void {
			
			
			trace ("RECONNECT ");
			
			socket.close();
			callbacks = new Object ();
			
			var maxCount:int = 20;
			
			if (!isPossibleConnected){
				maxCount = 2;
			}
				
			if (connectCoun < maxCount){
				
				if (si != -1){
					clearInterval(si);
				}
				
				si = setInterval(onReconnect,5000);
				
				connectCoun++;
			}else{
				close();
				
				dispatchEvent(new TFlashSocketClientEvent (TFlashSocketClientEvent.ERROR_CONNECT));
			}
		}
		
		
		
		private function onReconnect ():void {
			if (si != -1){
				clearInterval(si);
				si = -1;
			}
			this.socket.connect(host,port);
		}
		
		
        public function socketConnected(event:Event):void
        {
			
			isPossibleConnected = true;
			
			if (si != -1){
				clearInterval(si);
				si = -1;
			}
			
			dispatchEvent(event.clone());
			connectCoun = 0;
			
			dataFlust ();
        }
		
		private function sockeCloseHandler (event:Event):void {
			trace("Error close connecting:" + event);
			reconnect();
			
		}
		

        public function socketError(event:IOErrorEvent):void
        {
            trace("Error Connecting:" + event);
			dispatchEvent(event.clone());
			reconnect();
        }

        public function socketSecurityError(event:SecurityErrorEvent):void
        {
            trace("Security Error Connecting:" + event);
			dispatchEvent(event.clone());
            reconnect();
        }

        public function socketDataHandler(event:ProgressEvent):void
        {
			
			//trace ("socketDataHandler:"+this.socket.bytesAvailable);
			
			var ba:ByteArray = new ByteArray ();
			ba.length = this.socket.bytesAvailable;
			ba.position = 0;
			this.socket.readBytes(ba);
			readData (ba);
        }
		
		
		private function readData (ba:ByteArray = null):void {
			
			if (ba){
				responseBuffer.position = responseBuffer.length;
				responseBuffer.writeBytes(ba);
			}
			responseBuffer.position = 0;
			
			if (responseBuffer.bytesAvailable>3){
				//trace ("bufferLen "+responseBuffer.bytesAvailable);
				
				var len = responseBuffer.readInt();
				
				//trace ("LEN "+len);
				
				/*				var i:int = 0;
				while (responseBuffer.bytesAvailable > 3){
				i++;
				trace (">>> "+i+": "+responseBuffer.readInt())
				}
				
				responseBuffer.position = 0;
				
				trace ("в байтах")
				
				var i:int = 0;
				while (responseBuffer.bytesAvailable){
				i++;
				trace (">>> "+i+": "+responseBuffer.readByte())
				}*/
				
				//responseBuffer.position = 0;
				
				if (responseBuffer.bytesAvailable >= len+4){
					
					//trace("Got Data call:" +callback);
					//responseBuffer.position = 4;
					seqId = responseBuffer.readInt();
					
					var callback:Function = callbacks[seqId]
					delete callbacks[seqId];
					//trace("READ : seqId " + seqId);
					
					if (callback != null)
					{
						try {
							callback(null);
						}catch (e:Error){
							dispatchEvent(new THttpClientEvent(e.name, e));
						}
					};
					
					ba = new ByteArray ();
					responseBuffer.position = 0;
					ba.writeBytes(responseBuffer,len+8);
					responseBuffer = ba;
					
					readData ()
					
				}
			}
		}
		

        public override function flush(seqId:int,callback:Function = null):void
        {
           // trace("FLUSH : seqId " + seqId);
			callbacks[seqId] = callback;
/*            this.output.writeUTF(this.obuffer.length.toString(16));
            this.output.writeBytes(CRLF);*/
			
			
			dataFlust ();
        }
		
		private function dataFlust ():void {
			if (this.socket.connected){
				
				this.obuffer.position = 0;
				
				
				//trace ("this.obuffer "+this.obuffer.bytesAvailable);
				
				this.socket.writeBytes(this.obuffer);
				
				/*this.output.writeBytes(CRLF);*/
				this.socket.flush();
				// waiting for  new Flex sdk 3.5
				//this.obuffer.clear();
				this.obuffer = new ByteArray();
			}
		}
		

        public override function isOpen():Boolean
        {
            return (this.socket == null ? false : this.socket.connected);
        }

    }
}