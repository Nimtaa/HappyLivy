import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
public class AgainClient {
    private LivyClient client = null;
    private InnerLivyJob innerjob;
    public AgainClient() {
         innerjob = new InnerLivyJob(1);
        try {
            client = new LivyClientBuilder().setURI(new URI("http","nima:ta","127.0.0.1",8998,"","",""))
                    .setConf("livy.rsc.rpc.max.size","2000000000") //enter max limit size
                    .setConf("livy.rsc.channel.log.level","ERROR")
                    .setConf("livy.rsc.server.connect.timeout","150000000ms")
                    .setConf("livy.client.http.connection.timeout","150000000ms")
                    .setConf("livy.spark.master","local")
//                    .setConf("livy.spark.deploy-mode","client")
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        AgainClient s = new AgainClient();
        String thisJAR = "./target/apachelivttest-1.0-SNAPSHOT.jar";
        String newJAR = "./out/artifacts/Apache_Livy_Again_jar";
        try {
            s.client.uploadJar(new File(thisJAR));
            System.err.printf("Running PiJob with %d samples...\n",1);
            double pi = 0;
            pi = s.client.submit(new LivyJob(1)).get();
            System.out.println("Pi is roughly: " + pi);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        finally {
            s.client.stop(true);
        }
    }
    private class InnerLivyJob implements Job<Double>, Function<Integer, Integer>,
            Function2<Integer, Integer, Integer> ,Serializable {
        private final int samples;
        public InnerLivyJob(int samples) {
            this.samples = samples;
            System.out.println("this is in livy job constructor");
        }
        @Override
        public Double call(JobContext ctx) throws Exception {
            System.out.println("this is in jobcontext livy job");
            List<Integer> sampleList = new ArrayList<Integer>();
            for (int i = 0; i < samples; i++) {
                sampleList.add(i + 1);
            }
            return 4.0d * ctx.sc().parallelize(sampleList).map(this).reduce(this) / samples;
        }
        @Override
        public Integer call(Integer v1) {
            double x = Math.random();
            double y = Math.random();
            return (x*x + y*y < 1) ? 1 : 0;
        }
        @Override
        public Integer call(Integer v1, Integer v2) {
            return v1 + v2;
        }
    }



}
