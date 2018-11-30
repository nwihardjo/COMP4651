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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Iterators;

public class AnalyzeBigramFrequency {

	private static final String INPUT = "input";
	private static final String WORD = "word";

	public static Map<PairOfStrings, FloatWritable> readDirectory(Path path) {
		FileSystem fs;

		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Unable to access the file system!");
		}

		return readDirectory(path, fs, Integer.MAX_VALUE);
	}

	public static Map<PairOfStrings, FloatWritable> readDirectory(Path path,
			FileSystem fs, int max) {
		Map<PairOfStrings, FloatWritable> map = new HashMap<PairOfStrings, FloatWritable>();

		try {
			FileStatus[] stat = fs.listStatus(path);
			for (int i = 0; i < stat.length; i++) {
				// skip '_log' directory
				if (stat[i].getPath().getName().startsWith("_")) {
					continue;
				}

				Map<PairOfStrings, FloatWritable> pairs = readFile(
						stat[i].getPath(), fs, max);
				map.putAll(pairs);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading the file system!");
		}

		return map;
	}

	public static Map<PairOfStrings, FloatWritable> readFile(Path path,
			FileSystem fs, int max) throws IOException {
		Map<PairOfStrings, FloatWritable> map = new HashMap<PairOfStrings, FloatWritable>();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(path)));
		try {
			String line;
			line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("\\s+");
				PairOfStrings bigram = null;
				FloatWritable freq = null;
				if (tokens.length == 2) {
					bigram = new PairOfStrings(tokens[0], "");
					freq = new FloatWritable(Float.parseFloat(tokens[1]));
				} else if (tokens.length == 3) {
					bigram = new PairOfStrings(tokens[0], tokens[1]);
					freq = new FloatWritable(Float.parseFloat(tokens[2]));
				} else {
					throw new IOException(
							"Error parsing the line: expect 2 or 3 terms delimited by tab!");
				}
				map.put(bigram, freq);
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
		
		options.addOption(OptionBuilder.withArgName("word").hasArg()
				.withDescription("input path").create(WORD));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(WORD)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter
					.printHelp(AnalyzeBigramFrequency.class.getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		Path path = new Path(inputPath);
		Map<PairOfStrings, FloatWritable> bigramFrequency = readDirectory(path);
			
		String word = cmdline.getOptionValue(WORD);
		Map<PairOfStrings, FloatWritable> bigrams = new HashMap<PairOfStrings, FloatWritable>();
		for (Map.Entry<PairOfStrings, FloatWritable> entry : bigramFrequency
				.entrySet()) {
			if (entry.getKey().getLeftElement().equals(word)) {
				bigrams.put(entry.getKey(), entry.getValue());
			}
		}

		bigrams = MapUtil.sortByValue(bigrams);

		System.out.println("Ten most frequent bigrams starting with " + word + ":");

		Iterator<Map.Entry<PairOfStrings, FloatWritable>> iter = Iterators.limit(
				bigrams.entrySet().iterator(), 11);
		while (iter.hasNext()) {
			Map.Entry<PairOfStrings, FloatWritable> b = iter.next();
			System.out.println(b.getKey() + "\t" + b.getValue());
		}
	}
}
