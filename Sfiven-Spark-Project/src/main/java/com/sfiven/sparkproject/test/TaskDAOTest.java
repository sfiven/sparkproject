package com.sfiven.sparkproject.test;

import com.sfiven.sparkproject.dao.ITaskDAO;
import com.sfiven.sparkproject.dao.factory.DAOFactory;
import com.sfiven.sparkproject.domain.Task;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDAOTest {
	
	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());  
	}
	
}
