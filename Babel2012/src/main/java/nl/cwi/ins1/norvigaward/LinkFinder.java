package nl.cwi.ins1.norvigaward;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class LinkFinder extends EvalFunc<DataBag> {

	private static Logger log = Logger.getLogger(LinkFinder.class);

	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();

	private static final Pattern p = Pattern
			.compile("(https?://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|])");

	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try {

			Matcher m = p.matcher((String) input.get(0));

			DataBag output = mBagFactory.newDefaultBag();
			while (m.find()) {
				output.add(mTupleFactory.newTuple(m.group(1)));
			}
			return output;
		} catch (Exception ee) {
			log.warn("Unable to process tuple", ee);
		}
		return mBagFactory.newDefaultBag();
	}

}
