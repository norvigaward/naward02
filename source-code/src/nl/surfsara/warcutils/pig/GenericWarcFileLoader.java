/**
 * Copyright 2014 SURFsara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.surfsara.warcutils.pig;

import java.io.IOException;
import java.net.URL;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;

/**
 * Pig load function for regular (or compressed) warc, wat and wet files. Values
 * from the reader are returned as WarcRecords from the Java Web Archive
 * Toolkit.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 * @author Jeroen Schot <jeroen.schot@surfsara.nl>
 */
public abstract class GenericWarcFileLoader extends LoadFunc {

	private RecordReader<LongWritable, WarcRecord> in;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();
	private BagFactory mBagFactory = BagFactory.getInstance();

	@Override
	public Tuple getNext() throws IOException {
		WarcRecord warcRecord = null;
		try {
			while (in.nextKeyValue()) { 
				warcRecord = in.getCurrentValue();
				String domain = null;

				if (warcRecord.header.warcTargetUriStr != null) {
					URL myUrl = new URL(warcRecord.header.warcTargetUriStr);
					String[] domainNameParts = myUrl.getHost().split("\\.");
					domain = domainNameParts[domainNameParts.length - 1];
				}

				HttpHeader httpheader = warcRecord.getHttpHeader();
				if (httpheader == null || httpheader.payloadLength == 0) 
					continue;

				StringBuffer sb = new StringBuffer();

				Payload pl = warcRecord.getPayload();
				while (pl.getRemaining() > 0) {
					sb.append(pl.getInputStream().readLine());
				}
				pl.getInputStreamComplete().close();
				pl.close();

				DataBag langB = mBagFactory.newDefaultBag();
				boolean correct = false;
				String s = sb.toString();
				if (s.contains(" C ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("C");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Java ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Java");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Objective-C ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("ObjectiveC");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" C++ ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("CPP");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" C# ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("CS");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Visual Basic")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("VisualBasic");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" PHP ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("PHP");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Python ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Python");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" JavaScript ")
						&& !s.toLowerCase().contains("javascript disabled")
						&& !s.toLowerCase().contains("javascript enabled")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("JavaScript");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" SQL ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("SQL");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Perl ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Perl");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Ruby ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Ruby");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" ActionScript ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("ActionScript");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" F# ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("FS");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Lisp ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Lisp");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Delphi ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Delphi");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Pascal ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Pascal");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" MATLAB ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("MATLAB");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Assembly ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Assembly");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Scala ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Scala");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Ada ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Ada");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Fortran ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Fortran");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Haskell ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Haskell");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" Groovy ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("Groovy");
					langB.add(t);
					correct = true;
				}

				if (s.contains(" PostScript ")) {
					Tuple t = mTupleFactory.newTuple();
					t.append("PostScript");
					langB.add(t);
					correct = true;
				}

				if (!correct)
					continue;

				/*
				 * You can expand this loader by returning values from the
				 * warcRecord as needed.
				 */
				Tuple t = mTupleFactory.newTuple(2);

				t.set(0, domain);
				t.set(1, langB);

				return t;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return null;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void prepareToRead(RecordReader reader, PigSplit arg1)
			throws IOException {
		in = reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}
}
