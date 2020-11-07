import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class leastNMapper extends Mapper<Object, Text, Text, LongWritable> {

	private HashMap<String, Long> hmap;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		hmap = new HashMap<String, Long>();
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// input data format => movie_name
		// no_of_views (tab separated)
		// we split the input data
		StringTokenizer itr = new StringTokenizer(value.toString());

		String word;
		long count;
		while (itr.hasMoreTokens()) {
			word = itr.nextToken();
			count = hmap.containsKey(word) ? hmap.get(word) + 1 : 1;
			hmap.put(word, count);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Entry<String, Long> entry : hmap.entrySet()) {

			String name = entry.getKey();
			Long count = entry.getValue();

			context.write(new Text(name), new LongWritable(count));
		}
	}
}