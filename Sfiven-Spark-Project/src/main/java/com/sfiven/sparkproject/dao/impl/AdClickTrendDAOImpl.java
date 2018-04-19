package com.sfiven.sparkproject.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.sfiven.sparkproject.dao.IAdClickTrendDAO;
import com.sfiven.sparkproject.domain.AdClickTrend;
import com.sfiven.sparkproject.jdbc.JDBCHelper;
import com.sfiven.sparkproject.model.AdClickTrendQueryResult;

/**
 * 广告点击趋势DAO实现类
 * @author Administrator
 *
 */
public class AdClickTrendDAOImpl implements IAdClickTrendDAO {

	public void updateBatch(List<AdClickTrend> adClickTrends) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		List<AdClickTrend> updateAdClickTrends = new ArrayList<AdClickTrend>();
		List<AdClickTrend> insertAdClickTrends = new ArrayList<AdClickTrend>();
		
		String selectSQL = "SELECT count(*) "
				+ "FROM ad_click_trend "
				+ "WHERE date=? "
				+ "AND hour=? "
				+ "AND minute=? "
				+ "AND ad_id=?";
		
		for(AdClickTrend adClickTrend : adClickTrends) {
			final AdClickTrendQueryResult queryResult = new AdClickTrendQueryResult();
			
			Object[] params = new Object[]{adClickTrend.getDate(),
					adClickTrend.getHour(),
					adClickTrend.getMinute(),
					adClickTrend.getAdid()};
			
			jdbcHelper.executeQuery(selectSQL, params, new JDBCHelper.QueryCallback() {  
				
				public void process(ResultSet rs) throws Exception {
					if(rs.next()) {
						int count = rs.getInt(1);
						queryResult.setCount(count); 
					}
				}
				
			});
			
			int count = queryResult.getCount();
			if(count > 0) {
				updateAdClickTrends.add(adClickTrend);
			} else {
				insertAdClickTrends.add(adClickTrend);
			}
		}
		
		// 执行批量更新操作
		String updateSQL = "UPDATE ad_click_trend SET click_count=? "
				+ "WHERE date=? "
				+ "AND hour=? "
				+ "AND minute=? "
				+ "AND ad_id=?";
		
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		
		for(AdClickTrend adClickTrend : updateAdClickTrends) {
			Object[] params = new Object[]{adClickTrend.getClickCount(), 
					adClickTrend.getDate(),
					adClickTrend.getHour(),
					adClickTrend.getMinute(),
					adClickTrend.getAdid()};  
			updateParamsList.add(params);
		}
		
		jdbcHelper.executeBatch(updateSQL, updateParamsList);
		
		// 执行批量更新操作
		String insertSQL = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)";
		
		List<Object[]> insertParamsList = new ArrayList<Object[]>();
		
		for(AdClickTrend adClickTrend : insertAdClickTrends) {
			Object[] params = new Object[]{adClickTrend.getDate(),
					adClickTrend.getHour(),
					adClickTrend.getMinute(),
					adClickTrend.getAdid(),
					adClickTrend.getClickCount()};    
			insertParamsList.add(params);
		}
		
		jdbcHelper.executeBatch(insertSQL, insertParamsList);
	}

}
