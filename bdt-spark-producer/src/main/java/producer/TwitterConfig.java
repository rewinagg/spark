package producer;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

@Configuration
public class TwitterConfig {
    @Bean
    public TwitterTemplate twitterTemplate() {
        return new TwitterTemplate("wq41b14nM6wE9UdLYGcV9C81I", "DWpdpJslCjqY4lqvVP1pk9MwId2ebSQ6IValPdud9yKxTDu38v",
        		"744257963741720576-0iRSGfNMKEqWbDqHBdc3WZeKTJgdAxM","YE3P0CyV3ucOFKb5sbBPsVcfTNVwFo10oDqm8fTqsiK5W");
    }
}
