package com.rswestmoreland.graylog2.plugin;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.graylog2.plugin.Message;

/**
 * Optimized sender
 */
public abstract class MessageSender {
	protected FileWrite fileWrite;
	protected DelimitedFileOutput instance;
	protected Logger LOG;

	MessageSender(FileWrite fileWrite, DelimitedFileOutput instance){
		this.fileWrite = fileWrite;
		this.instance = instance;
	}
	
	public void Debug(Message msg) {
		if (instance.debug == true) {
			try {
				fileWrite.DebugFile(msg.getFieldNames().toString(), Long.toString(Thread.currentThread().getId()));
			} catch (IOException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	}
	
	abstract void send(Message msg);
}