/**
 * Created with IntelliJ IDEA.
 * User: gxm
 * Date: 2019/10/8
 * Time: 8:29
 * To change this template use File | Settings | File Templates.
 * Description:
 **/
package com.flow.inverse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class InverseIndexRunner {
    private static final Log logger = LogFactory.getLog(InverseIndexRunner.class);

    public static void main(String[] args) throws Exception {

//        int resStepOne = ToolRunner.run(new Configuration(), new InverseIndexStepOne(), args);
//        if (resStepOne == 0) {
//            logger.info("step one success");
//        } else {
//            System.exit(resStepOne);
//        }

        int resStepTwo = ToolRunner.run(new Configuration(), new InverseIndexStepTwo(), args);
        if (resStepTwo == 0) {
            logger.info(" step two success");
        }
        System.exit(resStepTwo);

    }
}
