package sparkproject.dao.factory;

import sparkproject.dao.IAdBlacklistDAO;
import sparkproject.dao.IAdClickTrendDAO;
import sparkproject.dao.IAdProvinceTop3DAO;
import sparkproject.dao.IAdStatDAO;
import sparkproject.dao.IAdUserClickCountDAO;
import sparkproject.dao.IAreaTop3ProductDAO;
import sparkproject.dao.IPageSplitConvertRateDAO;
import sparkproject.dao.ISessionAggrStatDAO;
import sparkproject.dao.ISessionDetailDAO;
import sparkproject.dao.ISessionRandomExtractDAO;
import sparkproject.dao.ITaskDAO;
import sparkproject.dao.ITop10CategoryDAO;
import sparkproject.dao.ITop10SessionDAO;
import sparkproject.dao.impl.AdBlacklistDAOImpl;
import sparkproject.dao.impl.AdClickTrendDAOImpl;
import sparkproject.dao.impl.AdProvinceTop3DAOImpl;
import sparkproject.dao.impl.AdStatDAOImpl;
import sparkproject.dao.impl.AdUserClickCountDAOImpl;
import sparkproject.dao.impl.AreaTop3ProductDAOImpl;
import sparkproject.dao.impl.PageSplitConvertRateDAOImpl;
import sparkproject.dao.impl.SessionAggrStatDAOImpl;
import sparkproject.dao.impl.SessionDetailDAOImpl;
import sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import sparkproject.dao.impl.TaskDAOImpl;
import sparkproject.dao.impl.Top10CategoryDAOImpl;
import sparkproject.dao.impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}
	
	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
	
}
