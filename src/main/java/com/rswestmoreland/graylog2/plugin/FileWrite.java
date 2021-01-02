/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.rswestmoreland.graylog2.plugin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * @author rwestmoreland
 */
public class FileWrite {

	public AtomicBoolean bufferPair = new AtomicBoolean(false);
	public BufferedWriter outputBuffer0;
	public BufferedWriter outputBuffer1;
	public AtomicInteger rotateNum = new AtomicInteger(0);
	public AtomicInteger rotateCounter = new AtomicInteger(0);

	private DelimitedFileOutput instance;
	private String currentPath = "";

	private static final Logger LOG = Logger.getLogger(FileWrite.class.getName());

	FileWrite(DelimitedFileOutput instance) {
		this.instance = instance;
	}

	public Runnable PeriodicRotate = new Runnable() {
		@Override
		public void run() {
			try {
				if (instance.debug == true) {
					ZonedDateTime utc = ZonedDateTime.now();
					StringBuilder out = new StringBuilder();
					out.append("DEBUG: Rotating file every ").append(Integer.toString(instance.rotateinterval))
							.append(" seconds, using pid ").append(instance.pid).append(" thread ")
							.append(Long.toString(Thread.currentThread().getId())).append(" at ")
							.append(utc.toString());
					WriteFile(out);

					if (bufferPair.get() == false) {
						outputBuffer0.flush();
					} else {
						outputBuffer1.flush();
					}
				}

				NewFile();
			} catch (IOException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	};

	public void CounterRotate() {
		try {
			if (instance.debug == true) {
				ZonedDateTime utc = ZonedDateTime.now();
				StringBuilder out = new StringBuilder();
				out.append("DEBUG: Rotating file every ").append(Integer.toString(instance.rotateinterval))
						.append(" lines, using pid ").append(instance.pid).append(" thread ")
						.append(Long.toString(Thread.currentThread().getId())).append(" at ").append(utc.toString());

				if (bufferPair.get() == false) {
					outputBuffer0.append(out).append(instance.endlineSequence);
					outputBuffer0.flush();
				} else {
					outputBuffer1.append(out).append(instance.endlineSequence);
					outputBuffer1.flush();
				}
			}

			NewFile();
			rotateCounter.set(0);
		} catch (IOException ex) {
			LOG.log(Level.SEVERE, null, ex);
		}
	};

	public Runnable PeriodicFlush = new Runnable() {
		@Override
		public void run() {
			try {
				if (instance.debug == true) {
					ZonedDateTime utc = ZonedDateTime.now();
					StringBuilder out = new StringBuilder();
					out.append("DEBUG: Flushing ");
					if (bufferPair.get() == false) {
						out.append("buffer0");
					} else {
						out.append("buffer1");
					}
					out.append(" every ").append(Integer.toString(instance.flushinterval))
							.append(" seconds, using pid ").append(instance.pid).append(" thread ")
							.append(Long.toString(Thread.currentThread().getId())).append(" at ")
							.append(utc.toString());
					WriteFile(out);
				}

				if (bufferPair.get() == false) {
					outputBuffer0.flush();
				} else {
					outputBuffer1.flush();
				}

			} catch (IOException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		}
	};

	public void NewFile() throws IOException {

		long unixTime = System.currentTimeMillis() / 1000L;
		String updatedPath = instance.filepath.replace("$PID", instance.pid)
				.replace("$THREAD", Long.toString(Thread.currentThread().getId()))
				.replace("$HOST", instance.host).replace("$NODE", instance.node)
				.replace("$ROTATE", Integer.toString(rotateNum.get()))
				.replace("$PADDED", String.format("%06d", rotateNum.get())).replace("$EPOCH", Long.toString(unixTime));

		File newPath = new File(updatedPath).getAbsoluteFile();
		File nextPath = new File(updatedPath + instance.filedone).getAbsoluteFile();

		if (!newPath.isFile() && !nextPath.isFile()) {
			try {
				newPath.getParentFile().mkdirs();
				newPath.createNewFile();
			} catch (IOException ex) {
				LOG.log(Level.SEVERE, null, ex);
			}
		} else {
			if (instance.filepath.contains("$ROTATE") || instance.filepath.contains("$PADDED")) {
				int limitTries = 0;

				while ((newPath.isFile() || nextPath.isFile()) && limitTries < 1000000) {
					if (rotateNum.incrementAndGet() >= 1000000) {
						rotateNum.set(0);
					}

					updatedPath = instance.filepath.replace("$PID", instance.pid)
							.replace("$THREAD", Long.toString(Thread.currentThread().getId()))
							.replace("$HOST", instance.host).replace("$NODE", instance.node)
							.replace("$ROTATE", Integer.toString(rotateNum.get()))
							.replace("$PADDED", String.format("%06d", rotateNum.get()))
							.replace("$EPOCH", Long.toString(unixTime));

					newPath = new File(updatedPath).getAbsoluteFile();
					nextPath = new File(updatedPath + instance.filedone).getAbsoluteFile();
					limitTries++;
				}
			}
		}

		if (instance.filepath.contains("$ROTATE") || instance.filepath.contains("$PADDED")) {
			if (rotateNum.incrementAndGet() >= 1000000) {
				rotateNum.set(0);
			}
		}

		OutputStreamWriter fileOutput;
		switch (instance.compress) {
		case "gzip":
			GZIPOutputStream gzip = new GZIPOutputStream(new FileOutputStream(newPath, true),
					instance.buffersize, true);
			fileOutput = new OutputStreamWriter(gzip, "UTF-8");
			break;
		case "gzip_fast":
			GZIPOutputStream gzip1 = new GZIPOutputStream(new FileOutputStream(newPath, true),
					instance.buffersize, true) {
				{
					def.setLevel(1);
				}
			};
			fileOutput = new OutputStreamWriter(gzip1, "UTF-8");
			break;
		case "gzip_max":
			GZIPOutputStream gzip9 = new GZIPOutputStream(new FileOutputStream(newPath, true),
					instance.buffersize, true) {
				{
					def.setLevel(9);
				}
			};
			fileOutput = new OutputStreamWriter(gzip9, "UTF-8");
			break;
		default:
			fileOutput = new OutputStreamWriter(new FileOutputStream(newPath, true), "UTF-8");
			break;
		}

		if (bufferPair.get() == true) {
			outputBuffer0 = new BufferedWriter(fileOutput, instance.buffersize);
			if (outputBuffer1 != null) {
				outputBuffer1.flush();
				outputBuffer1.close();

			}
			bufferPair.set(false);
		} else {
			outputBuffer1 = new BufferedWriter(fileOutput, instance.buffersize);
			if (outputBuffer0 != null) {
				outputBuffer0.flush();
				outputBuffer0.close();
			}
			bufferPair.set(true);
		}

		RenameFile();
		currentPath = updatedPath;

	}

	public void RenameFile() {
		if (!currentPath.equals("")) {
			if (!instance.filedone.equals("")) {
				File oldPath = new File(currentPath).getAbsoluteFile();
				if (oldPath.isFile()) {
					File renamePath = new File(currentPath + instance.filedone).getAbsoluteFile();
					oldPath.renameTo(renamePath);
				}
			}
		}
	}

	public void CloseFile() throws IOException {
		if (bufferPair.get() == false) {
			outputBuffer0.flush();
			outputBuffer0.close();
		} else {
			outputBuffer1.flush();
			outputBuffer1.close();
		}
		RenameFile();
		outputBuffer0 = null;
		outputBuffer1 = null;
	}

	public void DebugFile(String fieldNames, String threadId) throws IOException {
		StringBuilder out = new StringBuilder();

		if (instance.debug == true) {
			out.append("DEBUG: Writing ").append(instance.fileformat);
			out.append(" with pid ").append(instance.pid).append(" thread ").append(threadId)
					.append(", trying fields [").append(instance.fields).append("] with fields available: ")
					.append(fieldNames.replaceAll(" ", ""));
		}

		WriteFile(out);
	}

	public synchronized void WriteFile(StringBuilder output) throws IOException {
		if (bufferPair.get() == false) {
			// outputBuffer0.append(Integer.toString(rotateCounter.get())).append("\t");
			outputBuffer0.append(output).append(instance.endlineSequence);
		} else {
			// outputBuffer1.append(Integer.toString(rotateCounter.get())).append("\t");
			outputBuffer1.append(output).append(instance.endlineSequence);
		}
		if (instance.countLines == true) {
			if (rotateCounter.incrementAndGet() == instance.rotateinterval) {
				CounterRotate();
			}
		}
	}
}
