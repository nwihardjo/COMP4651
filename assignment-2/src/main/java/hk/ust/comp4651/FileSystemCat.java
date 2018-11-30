package hk.ust.comp4651;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/*
 * An Implementation of "hadoop fs -cat"
 * Print out the contents of a given file to System.out
 */
public class FileSystemCat {

	private static final int BufferSize = 4096;

	public static void main(String[] args) throws Exception {

		/*
		 * Validate that one argument is passed from the command line.
		 */
		if (args.length != 1) {
			System.err.printf("Usage: FileSystemCat <input file>\n");
			System.exit(-1);
		}

		/*
		 * Get the location of the input file
		 * URI starting with "file:///" stands for a local file
		 */
		String uri = args[0];
		/*
		 * A Configuration object encapsulates a client or server's configuration,
		 * which is set using configuration files read from the classpath, such
		 * as etc/hadoop/core-site.xml
		 */
		Configuration conf = new Configuration();
		/*
		 * FileSystem is a general filesystem API, the first step is to retrieve
		 * an instance for the filesystem we want to use, e.g., HDFS. This can
		 * be done by calling a static factory methods.
		 */
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			/*
			 * Invoke an open() method to get the input stream for a file
			 */
			in = fs.open(new Path(uri));
			/*
			 * IOUtils is a handy class that comes with Hadoop for IO
			 * operations. The first two arguments to the copyBytes() method
			 * respectively specify the input and output streams; the last two
			 * are the buffer size used for copying and whether to close the
			 * streams when the copy is complete. Here, we choose to close the
			 * input stream ourselves, and System.out doesn't need to be closed.
			 */
			IOUtils.copyBytes(in, System.out, BufferSize, false);
		} finally {
			IOUtils.closeStream(in);
		}

	}

}
