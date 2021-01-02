package com.rswestmoreland.graylog2.plugin;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.graylog2.plugin.Message;

/**
 * Formats fields into message text
 * 
 */
public class FormatSpace extends MessageSender {
	
	FormatSpace(FileWrite fileWrite, DelimitedFileOutput instance) {
		super(fileWrite, instance);
		LOG = Logger.getLogger(FormatSpace.class.getName());
	}

	@Override
	public void send(Message msg) {
		Debug(msg);

		StringBuilder out = new StringBuilder();

		for (int field = 0; field < instance.fieldListSize; field++) {
			out.append(msg.hasField(instance.fieldList[field]) == true
					? msg.getField(instance.fieldList[field]).toString()
					: "").append(" ");
		}
		out.append(msg.hasField(instance.fieldList[instance.fieldListSize]) == true
				? msg.getField(instance.fieldList[instance.fieldListSize]).toString()
				: "");

		try {
			fileWrite.WriteFile(out);
		} catch (IOException ex) {
			LOG.log(Level.SEVERE, null, ex);
		}
	}

}
