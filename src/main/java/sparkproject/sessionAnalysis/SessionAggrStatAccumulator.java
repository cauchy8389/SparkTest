package sparkproject.sessionAnalysis;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.util.AccumulatorV2;
import sparkproject.Constants;
import sparkproject.MockData;
import sparkproject.ParamUtils;

/**
 * session聚合统计Accumulator
 * 
 * 大家可以看到
 * 其实使用自己定义的一些数据格式，比如String，甚至说，我们可以自己定义model，自己定义的类（必须可序列化）
 * 然后呢，可以基于这种特殊的数据格式，可以实现自己复杂的分布式的计算逻辑
 * 各个task，分布式在运行，可以根据你的需求，task给Accumulator传入不同的值
 * 根据不同的值，去做复杂的逻辑
 * 
 * Spark Core里面很实用的高端技术
 * 
 * @author Administrator
 *
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String,String> {

	private static final long serialVersionUID = 6311074555136039130L;

	private String str = SessionAggrStatAccumulator.zero;

	final static String zero = Constants.SESSION_COUNT + "=0|"
			+ Constants.TIME_PERIOD_1s_3s + "=0|"
			+ Constants.TIME_PERIOD_4s_6s + "=0|"
			+ Constants.TIME_PERIOD_7s_9s + "=0|"
			+ Constants.TIME_PERIOD_10s_30s + "=0|"
			+ Constants.TIME_PERIOD_30s_60s + "=0|"
			+ Constants.TIME_PERIOD_1m_3m + "=0|"
			+ Constants.TIME_PERIOD_3m_10m + "=0|"
			+ Constants.TIME_PERIOD_10m_30m + "=0|"
			+ Constants.TIME_PERIOD_30m + "=0|"
			+ Constants.STEP_PERIOD_1_3 + "=0|"
			+ Constants.STEP_PERIOD_4_6 + "=0|"
			+ Constants.STEP_PERIOD_7_9 + "=0|"
			+ Constants.STEP_PERIOD_10_30 + "=0|"
			+ Constants.STEP_PERIOD_30_60 + "=0|"
			+ Constants.STEP_PERIOD_60 + "=0";
	/**
	 * zero方法，其实主要用于数据的初始化
	 * 那么，我们这里，就返回一个值，就是初始化中，所有范围区间的数量，都是0
	 * 各个范围区间的统计数量的拼接，还是采用一如既往的key=value|key=value的连接串的格式
	 */
	@Override
	public boolean isZero() {
		return StringUtils.equals(str,SessionAggrStatAccumulator.zero);
	}

	@Override
	public AccumulatorV2<String, String> copy() {
		SessionAggrStatAccumulator newAccumulator = new SessionAggrStatAccumulator();
		newAccumulator.str = this.str;
		return newAccumulator;
	}

	@Override
	public void reset() {
		str = SessionAggrStatAccumulator.zero;
	}

	@Override
	public void add(String v) {
		// 校验：v1为空的话，直接返回v2
		if(StringUtils.isEmpty(v)) {
			return;
		}

		// 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
		String oldValue = ParamUtils.getFieldFromConcatString(str, "\\|", v);
		if(oldValue != null) {
			// 将范围区间原有的值，累加1
			int newValue = Integer.valueOf(oldValue) + 1;
			// 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
			str = ParamUtils.setFieldIntoConcatString(str, "\\|", v, String.valueOf(newValue));
		}
	}

	@Override
	public void merge(AccumulatorV2<String, String> other) {
		//SessionAggrStatAccumulator o =(SessionAggrStatAccumulator)other;
		//str += o.str;
		addOtherValue(other, Constants.SESSION_COUNT);
		addOtherValue(other, Constants.TIME_PERIOD_1s_3s);
		addOtherValue(other, Constants.TIME_PERIOD_4s_6s);
		addOtherValue(other, Constants.TIME_PERIOD_7s_9s);
		addOtherValue(other, Constants.TIME_PERIOD_10s_30s);
		addOtherValue(other, Constants.TIME_PERIOD_30s_60s);
		addOtherValue(other, Constants.TIME_PERIOD_1m_3m);
		addOtherValue(other, Constants.TIME_PERIOD_3m_10m);
		addOtherValue(other, Constants.TIME_PERIOD_10m_30m);
		addOtherValue(other, Constants.TIME_PERIOD_30m);
		addOtherValue(other, Constants.STEP_PERIOD_1_3 );
		addOtherValue(other, Constants.STEP_PERIOD_4_6);
		addOtherValue(other,  Constants.STEP_PERIOD_7_9);
		addOtherValue(other, Constants.STEP_PERIOD_10_30 );
		addOtherValue(other, Constants.STEP_PERIOD_30_60);
		addOtherValue(other, Constants.STEP_PERIOD_60);
	}

	private void addOtherValue(AccumulatorV2<String, String> other, String vv){
		//String retValue = StringUtils.EMPTY;
		String otherValue = ParamUtils.getFieldFromConcatString(other.value(), "\\|", vv);
		String thisValue = ParamUtils.getFieldFromConcatString(str, "\\|", vv);


		if(otherValue != null) {
			if (thisValue == null) {
				str = ParamUtils.setFieldIntoConcatString(str, "\\|", vv, String.valueOf(otherValue));
			} else {
				int newValue = Integer.valueOf(otherValue) + Integer.valueOf(thisValue);
				// 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
				str = ParamUtils.setFieldIntoConcatString(str, "\\|", vv, String.valueOf(newValue));

			}
		}
	}

	@Override
	public String value() {
		return str;
	}
	
}
