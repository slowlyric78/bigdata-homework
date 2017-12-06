package com.epam.bd;

import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {
    String line1 = "445ed8056d8279f5419dc74dbc1a71bc\t20131019125009566\t1\tD11DNV9De8M\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; KB974488)\t27.38.216.*\t216\t219\t3\t7ed515fe566938ee6cfbb6ebb7ea4995\t4e613a7394f0f4c998904fc932d7a15c\tnull\tSports_F_Upright\t300\t250\tNa\tNa\t10\t7323\t294\t11\tnull\t2259\t13800,10059,10684,10075,10083,13042,10102,10024,10006,10111,11944,13403,10133,10063,10116,10125\n";
    String line2 = "b5d859a10a3a5588204f8b8e35ead2f\t20131019131808811\t1\tD12FlHCie6g\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\t113.67.164.*\t216\twrong\t3\tdd4270481b753dde29898e27c7c03920\t7d327804a2476a668c633b7fb857fddb\tnull\tEnt_F_Width1\t1000\t90\tNa\tNa\t70\t7336\t294\t255\tnull\t2259\t10048,10057,10067,10059,13496,10077,10093,10075,13042,10102,10006,10024,10148,10031,13776,10111,10127,10063,10116\n";
    String line3 = "859decac21c3c10f9d1bd481a4873cbc\t20131020143900493\t1\tDAKEc07jwqF\tMozilla/5.0 (iPad; CPU OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3\t183.3.202.*\t216\t217\t1\t704bdbbb6839e6bd83b7148ff8a0747b\t7d916c8bc32881e598117995962e1f01\tnull\tmm_15191080_2147689_8764813\t250\t250\tThirdView\tNa\t0\t7321\t294\t201\tnull\t2259\tnull\n";

    @Test
    public void parseLine() throws Exception {


        ImpressionEntry sample = new ImpressionEntry();
        ImpressionEntry testing = new ImpressionEntry();

        sample.set(219, 294, 1, "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; KB974488)");
        Utils.parseLine(testing, line1);
        Assert.assertEquals(sample, testing);

        sample.set(-1,0,0, "");
        Utils.parseLine(testing, line2);
        Assert.assertEquals(sample, testing);
    }

    @Test
    public void lookupCity() throws Exception {
        String city1 = "unknown"; //0
        String city2 = "handan"; //7
        String city3 = "baoding"; //9
        String city4 = "shenzhen"; //219
        String city5 = "missing(-1)"; //-1

        Assert.assertEquals(city1, Utils.lookupCity(0));
        Assert.assertEquals(city2, Utils.lookupCity(7));
        Assert.assertEquals(city3, Utils.lookupCity(9));
        Assert.assertEquals(city4, Utils.lookupCity(219));
        Assert.assertEquals(city5, Utils.lookupCity(-1));
    }

    @Test
    public void getPartitionId() throws Exception {
        ImpressionEntry ie = new ImpressionEntry();
        Utils.parseLine(ie, line1);
        Assert.assertEquals(0, Utils.getPartitionId(ie.getUserAgent()));
        Utils.parseLine(ie, line3);
        Assert.assertEquals(1, Utils.getPartitionId(ie.getUserAgent()));
    }
}
