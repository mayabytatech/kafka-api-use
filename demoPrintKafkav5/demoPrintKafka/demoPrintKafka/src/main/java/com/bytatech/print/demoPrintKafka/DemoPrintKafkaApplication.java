package com.bytatech.print.demoPrintKafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.bytatech.print.Booking;
import com.bytatech.print.Patient;
import com.bytatech.print.Payment;
import com.fasterxml.classmate.util.ResolvedTypeCache.Key;

import ch.qos.logback.core.net.SyslogOutputStream;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class DemoPrintKafkaApplication /*
										 * extends AbstractKafkaAvroSerDeConfig
										 */ {

	/**
	 * @param config
	 * @param props
	 */

	/*
	 * private static ConfigDef config; public
	 * DemoPrintKafkaApplication(ConfigDef config, Map<?, ?> props) {
	 * super(config, props); // TODO Auto-generated constructor stub }
	 */

	/**
	 * @param config
	 * @param props
	 */

	static final String REGISTER = "registering";
	static final String BOOKING = "booking";
	static final String PAYMENT = "payment";
	static final String BOOKING_STORE = "booking-stores";
	static final Long INACTIVITY_GAP = TimeUnit.MINUTES.toSeconds(1);
	private static final int RECORDS_TO_GENERATE = 5;
	
	  static final String PAYMENT_STORE = "payment-stores";

	public static void main(String[] args) {

		SpringApplication.run(DemoPrintKafkaApplication.class, args);

		final String bootstrapServers = "localhost:9092";

		final String schemaRegistryUrl = "http://localhost:8081";

		final KafkaStreams streams = createStreams(bootstrapServers, schemaRegistryUrl, "/tmp/booking");

		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}

	@SuppressWarnings("deprecation")
	private static KafkaStreams createStreams(String bootstrapServers, String schemaRegistryUrl, String stateDir) {

		final Properties streamsConfiguration = new Properties();

		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "booking");

		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// newly added
		/*
		 * streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG,
		 * "global-tables-example-client");
		 */
		streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
		streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// patientserde
		final SpecificAvroSerde<Patient> patientSerde = new SpecificAvroSerde<>();

		final Map<String, String> serdeConfig = Collections
				.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

		patientSerde.configure(serdeConfig, false);

		final KafkaProducer<Long, Patient> patientProducer = new KafkaProducer<>(streamsConfiguration,
				Serdes.Long().serializer(), patientSerde.serializer());

		// bookingserde
		final SpecificAvroSerde<Booking> bookingSerde = new SpecificAvroSerde<>();
		bookingSerde.configure(serdeConfig, false);

		final KafkaProducer<Long, Booking> bookingProducer = new KafkaProducer<>(streamsConfiguration,
				Serdes.Long().serializer(), bookingSerde.serializer());

		// paymentserde
		final SpecificAvroSerde<Payment> paymentSerde = new SpecificAvroSerde<>();
		paymentSerde.configure(serdeConfig, false);

		final KafkaProducer<Long, Payment> paymentProducer = new KafkaProducer<>(streamsConfiguration,
				Serdes.Long().serializer(), paymentSerde.serializer());

		//
		final List<Patient> allPatients = new ArrayList<>();
		final List<Booking> allBooking = new ArrayList<>();
		final List<Payment> allPayment = new ArrayList<>();
		String[] names = { "prasad", "ajith", "sruthy", "abi", "rafi", "karthi", "maya", "sarangi", "sanil", "jesseel",
				"sahal" };

		for (long i = 0; i < 5; i++) {

			final Patient patient = new Patient(names[(int) i], i + 1, "address" + i);

			final Booking booking = new Booking(i + 1, i, "doctor" + i, "hospital" + i, "date" + i);

			final Payment payment = new Payment("name" + i, i + 1, i + 1, "doctor" + i);

			allPatients.add(patient);
			allBooking.add(booking);
			allPayment.add(payment);

			patientProducer.send(new ProducerRecord<>(REGISTER, i, patient));
			bookingProducer.send(new ProducerRecord<>(BOOKING, i, booking));
			paymentProducer.send(new ProducerRecord<>(PAYMENT, i, payment));
		}

		patientProducer.close();
		bookingProducer.close();
		paymentProducer.close();

		// ...............................................................

		final StreamsBuilder builder = new StreamsBuilder();
		
		//....................Ktable.....................................................
		/*
		 * final KTable<Long,Patient>
		 * patientTable=builder.table(REGISTER,Consumed.with(Serdes.Long(),
		 * patientSerde)); patientTable.print();
		 */

		// j
		final KStream<Long, Patient> patientStream = builder.stream(REGISTER,
				Consumed.with(Serdes.Long(), patientSerde));
		// patientStream.print();
		

		// .......................................windowing....................................................................................

		/*
		 * patientStream.groupBy((key,value)->key,Serialized.with(Serdes.Long(),
		 * patientSerde)).windowedBy(SessionWindows.with(INACTIVITY_GAP)).reduce
		 * ((v1,v2)->v1).foreach((key,value)->System.out.println("key  "
		 * +key+"  value  "+value));
		 * patientStream.groupBy((key,value)->key,Serialized.with(Serdes.Long(),
		 * patientSerde)).windowedBy(SessionWindows.with(INACTIVITY_GAP)).count(
		 * ).foreach((key,value)->System.out.println("key  "+key+"  value  "
		 * +value));
		 */
		
		
		
		 final KStream<Long, Booking> booking =
				builder.stream(BOOKING,Consumed.with(Serdes.Long(), bookingSerde));
	
		 
		 
		// ...................................kstream -kstream joining.....................................................
		/*
		 * final KStream<Long, Booking> booking =
		 * builder.stream(BOOKING,Consumed.with(Serdes.Long(), bookingSerde));
		 * booking.print(); final KStream<Long, Payment> payment =
		 * patientStream.join(booking,(patient,booking1)->new
		 * Payment(patient.getName(),patient.getPatientid(),
		 * booking1.getBookingid(),booking1.getDoctorname())
		 * ,JoinWindows.of(TimeUnit.MINUTES.toMillis(5)), Joined.with(
		 * Serdes.Long(), patientSerde, bookingSerde) );
		 * 
		 * payment.print();
		 */

		final KStream<Long, Payment> payment = builder.stream(PAYMENT, Consumed.with(Serdes.Long(), paymentSerde));
		payment.print(Printed.toSysOut());

		// j global ktable of booking
		// GlobalKTable<Long, Booking> bookingstream =
		// builder.globalTable(BOOKING,Consumed.with(Serdes.Long(),
		// bookingSerde));

		
		
		
		
		// j.........................................joining using globalktable............................
		/*
		 * final KStream<Long, Payment> paymentStream =
		 * patientStream.join(bookingstream, (patientId, patient) -> patientId,
		 * (patient, booking) -> new
		 * Payment(patient.getName(),patient.getPatientid(),
		 * booking.getBookingid(),booking.getDoctorname()));
		 * paymentStream.to(PAYMENT, Produced.with(Serdes.Long(),
		 * paymentSerde));
		 */
		// paymentStream.print();

		
		
		
		
		// ..........................peek (didnt get actualworking).......................................
		/*
		 * KStream<Long,Payment> peek =
		 * paymentStream.peek((k,v)->System.out.println(v.getBookingid()+" : "+v
		 * .getDoctorname())); peek.print();
		 */

		
		
		
		// .....................................reduce method with groupby string field ...........................................
		/*
		KGroupedStream<String,Payment> groupedPayment = payment.groupBy((key,value)->value.getDoctorname().toString(), Serialized.with( Serdes.String(),
		  paymentSerde));
		 
		KTable<String,Payment> paymentTable = groupedPayment.reduce((v1,v2)->{v2.patientid=v1.patientid+v2.
				  patientid; return v2; });
		paymentTable.print();*/
		
		
		
		
		// ..............................group by stream with key and count............................
		/*
		 * paymentStream.groupBy((key,value)->key, Serialized.with(
		 * Serdes.Long(),
		 * paymentSerde)).count().toStream().foreach((key,value)->System.out.
		 * println("key  :"+key+" value:  "+value));
		 */
		
		
		
		
		
		//.....................................aggregate.............................................................
		
		        booking.groupBy((key,value)->key, Serialized.with(
				  Serdes.Long(),
				  bookingSerde)).aggregate(Booking::new,(key,value,aggregate)->new Booking(value.getBookingid()+aggregate.getBookingid(),value.getPatientid(),value.getDoctorname()+","+aggregate.getDoctorname(),value.getHospitalname(),value.getDate()),
						  Materialized.with(Serdes.Long(), bookingSerde)).print();
		
		
		
		
		
		//...........................Materialized view - failed...........................................
		
	             /*KTable<Long,Booking> bookingTable	=booking.groupBy((key,value)->key, Serialized.with(
				  Serdes.Long(),
				  bookingSerde)).aggregate(Booking::new,(key,value,aggregate)->new Booking(value.getBookingid()+aggregate.getBookingid(),value.getPatientid(),value.getDoctorname()+","+aggregate.getDoctorname(),value.getHospitalname(),value.getDate())
						  ,Materialized.as(PAYMENT_STORE).withValueSerde(bookingSerde));*/
		
		
		//..................................groupbykey................................................
		        /* streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
		    	KTable<Long,Long>paymenttable   = payment.groupByKey().count();
				paymenttable.print();*/
		        
		
		

		
		
		
		// paymentStream.foreach((key,value)->System.out.println("++++++++++++++++key:"+key+"+++++++++++++value: "+value));

		
		
		
		// .....................writing stream into TXT file.......................................
		// paymentStream.writeAsText("E:\\jhipster\\kafka-works\\stream.txt");
		
		log.info("payment++++++++++++++++++++++++++++++");

		/*
		 * final KStream<Long, Payment> payment = builder.stream(PAYMENT,
		 * Consumed.with(Serdes.Long(), paymentSerde));
		 * 
		 * payment.print();
		 */

		//.............. Normal print...........
		// patientStream.print();

		
		
		// ....................conditional filter of data.................................
		/*
		 * patientStream.filter((key,value) -> key>2).print();
		 */

		// .................branching...............................................
		/*
		 * KStream<Long, Patient>[] branches=patientStream.branch((key,value)->
		 * value.patientid<2,(key,value)-> value.name.equals("prasad"));
		 * branches[0].print(); branches[1].print();
		 */

		// ..............................print inverse of the condition...............
		/* patientStream.filterNot((key,value)->key>3).print(); */

		/*
		 * patientStream.to(REGISTER, Produced.with(Serdes.Long(),
		 * patientSerde));
		 * 
		 * patientStream.print();
		 */

		// ....................... sysout of key & value.....................................
		/*
		 * patientStream.foreach((key, value) -> System.out.println(key + " => "
		 * + value));
		 */

		
		
		
		// .................................selectkey...............................................
		 /*KStream<Booking,Payment> selectKeyPayment=payment.selectKey((key,value)->{
			 Booking b=new Booking();
			 b.setBookingid(value.getBookingid());
			 b.setDoctorname(value.getDoctorname());
			 return b;
		 });
		 selectKeyPayment.print();*/
		
		 
		 
		// ......................FlatMap more than one record of dta can be...................................
	
		/*KStream<String, String> transformed = patientStream.flatMap((key, value) -> {
			List<KeyValue<String, String>> linkedPatient = new LinkedList<>();
			linkedPatient.add(KeyValue.pair(value.getName().toString(), value.getAddress().toString()));
			linkedPatient.add(KeyValue.pa-ir(value.getName().toString(), value.getName().toString().toUpperCase()));
			return linkedPatient;
		});
		transformed.print();*/

		// .................................flatMapValues.................................................

		/*KStream<Long, Booking> hj=payment.flatMapValues(value -> {
.

			List<Booking> listBooking = new LinkedList<>();
			Booking b1 = new Booking();
			b1.setDoctorname(value.getDoctorname());
			b1.setPatientid(value.getPatientid());
			b1.setBookingid(value.getBookingid());
			listBooking.add( b1);

			return listBooking;

		});
		hj.print();*/
		
		
		
		
		// ........................map to another record of streams........................

		/*
		 * KStream<String, Patient> transformed = patientStream.map( (key,
		 * value) -> KeyValue.pair("1ll",p)); transformed.print();
		 */

		// ...............................mapValues......................................
		/*KStream<Long, String> mapvaluePaymet = payment.mapValues(value -> value.doctorname.toString());
		mapvaluePaymet.print();
        */
		
		return new KafkaStreams(builder.build(), new StreamsConfig(streamsConfiguration));
	}
}
