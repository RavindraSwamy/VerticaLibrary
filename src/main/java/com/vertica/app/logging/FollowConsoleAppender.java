package com.vertica.app.logging;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;


/**
 *
 * based on log4j 1.2.17 construction semantics
 *
 */

public class FollowConsoleAppender extends ConsoleAppender
{
	static
	{
		System.out.println("FollowConsoleAppender class loaded");
	}
	public FollowConsoleAppender() {
		super();
		System.out.println("FollowConsoleAppender() INITIALIZED");
		this.setFollow(true);
	}

	public FollowConsoleAppender(Layout layout) {
		super(layout);
		System.out.println("FollowConsoleAppender(Layout) INITIALIZED");
		this.setFollow(true);
		activateOptions();
	}

	public FollowConsoleAppender(Layout layout, String target) {
		super(layout, target);
		System.out.println("FollowConsoleAppender(Layout,String) INITIALIZED");
		this.setFollow(true);
		activateOptions();
	}

}
