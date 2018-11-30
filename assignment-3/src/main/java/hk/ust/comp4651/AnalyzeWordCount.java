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

public class AnalyzeWordCount {

	private static final String INPUT = "input";

	public static Map<String, IntWritable> readDirectory(Path path) {
		FileSystem fs;

		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			throw new RuntimeException("Unable to access the file system!");
		}

		return readDirectory(path, fs, Integer.MAX_VALUE);
	}

	public static Map<String, IntWritable> readDirectory(Path path,
			FileSystem fs, int max) {
		Map<String, IntWritable> map = new HashMap<String, IntWritable>();

		try {
			FileStatus[] stat = fs.listStatus(path);
			for (int i = 0; i < stat.length; i++) {
				// skip '_log' directory
				if (stat[i].getPath().getName().startsWith("_")) {
					continue;
				}

				Map<String, IntWritable> pairs = readFile(
						stat[i].getPath(), fs, max);
				map.putAll(pairs);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading the file system!");
		}
		
		return map;
	}

	public static Map<String, IntWritable> readFile(Path path,
			FileSystem fs, int max) throws IOException {
		Map<String, IntWritable> map = new HashMap<String, IntWritable>();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		try {
			String line;
			line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("\\s+");
				if (tokens.length != 2) {
					throw new IOException("Error parsing the line: expect 2 terms delimited by tab!");
				}
				String word = tokens[0];
				IntWritable count = new IntWritable(Integer.parseInt(tokens[1]));
				map.put(word, count);
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
			formatter.printHelp(AnalyzeWordCount.class.getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(-1);
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		Path path = new Path(inputPath);
		Map<String, IntWritable> wordCounts = readDirectory(path);
		wordCounts = MapUtil.sortByValue(wordCounts);

		int singletons = 0;
		int sum = 0;
		
		for (Map.Entry<String, IntWritable> entry: wordCounts.entrySet()) {
			sum += entry.getValue().get();
			if (entry.getValue().get() == 1) {
				singletons++;
			}
		}

		System.out.println("total number of unique words: "
				+ wordCounts.size());
		System.out.println("total number of words: " + sum);
		System.out.println("number of words that appear only once: "
				+ singletons);

		System.out.println("\nten most frequent words: ");

		Iterator<Map.Entry<String, IntWritable>> iter = Iterators.limit(
				wordCounts.entrySet().iterator(), 10);
		while (iter.hasNext()) {
			Map.Entry<String, IntWritable> b = iter.next();
			System.out.println(b.getKey() + "\t" + b.getValue());
		}
	}
}
