package com.vertica.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.vertica.test.date.DatePatternTest;

@RunWith(Suite.class)
@SuiteClasses({

	ApplicationPropertiesTest.class
	,QueryExecutorTest.class
	,DatePatternTest.class, QueryBuilderTest.class
	,ConnectionManagerTest.class
	//,NegativeDatePatternTest.class

})
public class AllTests {

	  // the class remains empty,
	  // used only as a holder for the above annotations

}
