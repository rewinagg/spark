package producer;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

@SpringBootApplication
public class TwitterProducerApplication implements CommandLineRunner {

	@Autowired
	StreamTweetEventService eventService;

	public static void main(String[] args) {
		SpringApplication.run(TwitterProducerApplication.class, args);
	}

	@Override
	public void run(String... strings) throws Exception {
		eventService.run();
	}
	
}
