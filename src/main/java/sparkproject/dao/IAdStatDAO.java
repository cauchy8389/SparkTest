package sparkproject.dao;

import java.util.List;

import sparkproject.domain.AdStat;

/**
 * 广告实时统计DAO接口
 * @author Administrator
 *
 */
public interface IAdStatDAO {

	void updateBatch(List<AdStat> adStats);
	
}
