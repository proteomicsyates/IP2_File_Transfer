package edu.scripps.yates.ip2tomassive;

import java.io.PrintStream;

import com.jcraft.jsch.SftpProgressMonitor;

import edu.scripps.yates.utilities.files.FileUtils;
import edu.scripps.yates.utilities.progresscounter.ProgressCounter;
import edu.scripps.yates.utilities.progresscounter.ProgressNumberFormatter;
import edu.scripps.yates.utilities.progresscounter.ProgressPrintingType;

public class MySftpProgressMonitor implements SftpProgressMonitor {
	private final ProgressCounter counter;
	private final PrintStream out;
	private String suffix = "";

	public MySftpProgressMonitor(PrintStream out) {
		this.out = out;
		counter = new ProgressCounter(0, ProgressPrintingType.PERCENTAGE_STEPS, 0, true);
		counter.setProgressNumberFormatter(new ProgressNumberFormatter() {

			@Override
			public String format(long number) {
				return FileUtils.getDescriptiveSizeFromBytes(number);
			}
		});
	}

	@Override
	public void init(int op, String src, String dest, long max) {
		counter.setTotal(max);

	}

	@Override
	public void end() {
		out.println("Transfer finished");

	}

	@Override
	public boolean count(long count) {
		counter.addCount(count);
		String printIfNecessary = counter.printIfNecessary();
		if (!"".equals(printIfNecessary)) {
			String message = printIfNecessary;
			if (suffix != null && !"".equals(suffix)) {
				message += " " + suffix;
			}
			out.print(message + "\r");
		}
		return true;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

}
