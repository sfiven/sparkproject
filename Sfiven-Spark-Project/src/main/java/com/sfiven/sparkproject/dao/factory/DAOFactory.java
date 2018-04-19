package com.sfiven.sparkproject.dao.factory;

import com.sfiven.sparkproject.dao.IAdBlacklistDAO;
import com.sfiven.sparkproject.dao.IAdClickTrendDAO;
import com.sfiven.sparkproject.dao.IAdProvinceTop3DAO;
import com.sfiven.sparkproject.dao.IAdStatDAO;
import com.sfiven.sparkproject.dao.IAdUserClickCountDAO;
import com.sfiven.sparkproject.dao.IAreaTop3ProductDAO;
import com.sfiven.sparkproject.dao.IPageSplitConvertRateDAO;
import com.sfiven.sparkproject.dao.ISessionAggrStatDAO;
import com.sfiven.sparkproject.dao.ISessionDetailDAO;
import com.sfiven.sparkproject.dao.ISessionRandomExtractDAO;
import com.sfiven.sparkproject.dao.ITaskDAO;
import com.sfiven.sparkproject.dao.ITop10CategoryDAO;
import com.sfiven.sparkproject.dao.ITop10SessionDAO;
import com.sfiven.sparkproject.dao.impl.AdBlacklistDAOImpl;
import com.sfiven.sparkproject.dao.impl.AdClickTrendDAOImpl;
import com.sfiven.sparkproject.dao.impl.AdProvinceTop3DAOImpl;
import com.sfiven.sparkproject.dao.impl.AdStatDAOImpl;
import com.sfiven.sparkproject.dao.impl.AdUserClickCountDAOImpl;
import com.sfiven.sparkproject.dao.impl.AreaTop3ProductDAOImpl;
import com.sfiven.sparkproject.dao.impl.PageSplitConvertRateDAOImpl;
import com.sfiven.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.sfiven.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.sfiven.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.sfiven.sparkproject.dao.impl.TaskDAOImpl;
import com.sfiven.sparkproject.dao.impl.Top10CategoryDAOImpl;
import com.sfiven.sparkproject.dao.impl.Top10SessionDAOImpl;

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
