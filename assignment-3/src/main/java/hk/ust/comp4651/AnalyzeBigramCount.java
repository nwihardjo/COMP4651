package hk.ust.comp4651;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Iterators;

public class AnalyzeBigramCount {

	private static final String INPUT = "input";

	public static Map<PairOfStrings, IntWritable> readDirectory(Path path) {
		FileSystem fs;

		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Unable to access the file system!");
		}

		return readDirectory(path, fs, Integer.MAX_VALUE);
	}

	public static Map<PairOfStrings, IntWritable> readDirectory(Path path,
			FileSystem fs, int max) {
		Map<PairOfStrings, IntWritable> map = new HashMap<PairOfStrings, IntWritable>();

		try {
			FileStatus[] stat = fs.listStatus(path);
			for (int i = 0; i < stat.length; i++) {
				// skip '_log' directory
				if (stat[i].getPath().getName().startsWith("_")) {
					continue;
				}

				Map<PairOfStrings, IntWritable> pairs = readFile(
						stat[i].getPath(), fs, max);
				map.putAll(pairs);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading the file system!");
		}
		
		return map;
	}

	public static Map<PairOfStrings, IntWritable> readFile(Path path,
			FileSystem fs, int max) throws IOException {
		Map<PairOfStrings, IntWritable> map = new HashMap<PairOfStrings, IntWritable>();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		try {
			String line;
			line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("\\s+");
				if (tokens.length != 3) {
					throw new IOException("Error parsing the line: expect 3 terms delimited by tab!");
				}
				PairOfStrings bigram = new PairOfStrings(tokens[0], tokens[1]);
				IntWritable count = new IntWritable(Integer.parseInt(tokens[2]));
				map.put(bigram, count);
				line = br.readLine();
			}
		} finally {
			br.close();
		}

		return map;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INPUT)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(AnalyzeBigramCount.class.getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		Path path = new Path(inputPath);
		Map<PairOfStrings, IntWritable> bigramCounts = readDirectory(path);
		bigramCounts = MapUtil.sortByValue(bigramCounts);

		int singletons = 0;
		int sum = 0;
		
		for (Map.Entry<PairOfStrings, IntWritable> entry: bigramCounts.entrySet()) {
			sum += entry.getValue().get();
			if (entry.getValue().get() == 1) {
				singletons++;
			}
		}

		System.out.println("total number of unique bigrams: "
				+ bigramCounts.size());
		System.out.println("total number of bigrams: " + sum);
		System.out.println("number of bigrams that appear only once: "
				+ singletons);

		System.out.println("\nten most frequent bigrams: ");

		Iterator<Map.Entry<PairOfStrings, IntWritable>> iter = Iterators.limit(
				bigramCounts.entrySet().iterator(), 10);
		while (iter.hasNext()) {
			Map.Entry<PairOfStrings, IntWritable> b = iter.next();
			System.out.println(b.getKey() + "\t" + b.getValue());
		}
	}
}
