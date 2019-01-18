package hk.ust.comp4651;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * Copy a file from a source to a destination.
 * The source and destination could be either the local filesystem
 * or HDFS.
 */
public class CopyFile {

	private static final int BufferSize = 4096;
	
	public static void main(String[] args) throws Exception {
		/*
		 * Validate that two arguments are passed from the command line.
		 */
		if (args.length != 2) {
			System.err.printf("Usage: CopyFile <src file> <dst file>\n");
			System.exit(-1);
		}

		String src = args[0];
		String dst = args[1];

		/*
		 * Prepare the input and output filesystems
		 */
		Configuration conf = new Configuration();
		FileSystem inFS = FileSystem.get(URI.create(src), conf);
		FileSystem outFS = FileSystem.get(URI.create(dst), conf);

		/*
		 * Prepare the input and output streams
		 */
		FSDataInputStream in = null;
		FSDataOutputStream out = null;

		// TODO: Your implementation goes here...		
		in = inFS.open(new Path(src));
		out = outFS.create(new Path(dst),  
				new Progressable() {
					public void progress() { System.out.println("."); }
		});
		IOUtils.copyBytes(in, out, conf, true);

//		inFS.copyToLocalFile(false,  new Path(src),  new Path(dst),  true);
		// System.out.println("Copying done");
		}
		
		
	}

