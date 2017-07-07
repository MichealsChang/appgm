package com.lanny.analysistest.util;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import java.io.IOException;

/**
 * 按照RDD的key存到多个文件中
 * 
 * @author 白鑫 2017年2月17日 下午3:51:51
 */
public class RDDMultipleTextOutputFormat<K,V> extends MultipleTextOutputFormat<K, V> {


	@Override
	protected K generateActualKey(K key, V value) {
		return null;
	}

	@Override
	protected String generateFileNameForKeyValue(K key, V value, String name) {
		return key.toString() + "/" + name;
	}

	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws FileAlreadyExistsException, InvalidJobConfException, IOException {
		// Ensure that the output directory is set and not already there
		Path outDir = getOutputPath(job);
		if (outDir == null && job.getNumReduceTasks() != 0) {
			throw new InvalidJobConfException("Output directory not set in JobConf.");
		}
		if (outDir != null) {
			FileSystem fs = outDir.getFileSystem(job);
			// normalize the output directory
			outDir = fs.makeQualified(outDir);
			setOutputPath(job, outDir);

			// get delegation token for the outDir's file system
			TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { outDir }, job);

			// check its existence
//			if (fs.exists(outDir)) {
//				throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
//			}
		}
	}

}
