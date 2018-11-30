package hk.ust.comp4651;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/*
 * Copy a local file to HDFS with progress
 */
public class CopyLocalFile {

	/*
	 * 4 KB buffer
	 */
	private static final int BufferSize = 4096;

	public static void main(String[] args) throws Exception {

		/*
		 * Validate that two arguments are passed from the command line.
		 */
		if (args.length != 2) {
			System.err
					.printf("Usage: CopyLocalFile <src file> <dst file>\n");
			System.exit(-1);
		}

		String localSrc = args[0];
		String dst = args[1];

		/*
		 * Prepare reading
		 */
		Configuration conf = new Configuration();
		LocalFileSystem localFS = LocalFileSystem.getLocal(conf);
		FSDataInputStream in = localFS.open(new Path(localSrc));

		/*
		 * Prepare writing
		 */
		FileSystem outFS = FileSystem.get(URI.create(dst), conf);
		/*
		 * The create() method takes a Path object for the file to be created
		 * and returns an output stream to write to. The second argument is
		 * optional. It is used to specify the action every time the progress()
		 * method is called by Hadoop, which is after each 64 KB packets of data
		 * is written to the datanode pipeline.
		 */
		FSDataOutputStream out = outFS.create(new Path(dst),
				new Progressable() {
					/*
					 * Print a dot whenever 64 KB of data has been written to
					 * the datanode pipeline.
					 */
					public void progress() {
						System.out.print(".");
					}
				});
		
		System.out.print("\n");

		/*
		 * Automatically close the stream after the write finishes.
		 */
		IOUtils.copyBytes(in, out, BufferSize, true);

	}

}
