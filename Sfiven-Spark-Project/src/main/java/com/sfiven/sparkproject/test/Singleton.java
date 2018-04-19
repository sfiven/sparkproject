package com.sfiven.sparkproject.test;

/**
 * 单例模式Demo
 * 
 * @author Administrator
 *
 */
public class Singleton {

	private static Singleton instance = null;

	private Singleton() {
		
	}
	
	/**
	 * @return
	 */
	public static Singleton getInstance() {

		if(instance == null) {
			synchronized(Singleton.class) {
				if(instance == null) {
					instance = new Singleton();
				}
			}
		}
		return instance;
	}
	
}
