package codenest.org.jdbctemplate;

import codenest.org.jdbctemplate.data.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.CallableStatementCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SpringBootApplication
public class JdbcApplication implements CommandLineRunner {
	private static final Logger log = LoggerFactory.getLogger(JdbcApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(JdbcApplication.class, args);
	}

	@Autowired
	JdbcTemplate jdbcTemplate;
	@Autowired
	NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Override
	public void run(String... args) throws Exception {
		log.info("Creating tables");
		jdbcTemplate.execute("DROP TABLE customers IF EXISTS");
		jdbcTemplate.execute("CREATE TABLE customers(" +
				"id SERIAL, first_name VARCHAR(255), last_name VARCHAR(255))");

		// Split up the array of whole names into an array of first/last names
		List<Object[]> splitUpNames = Arrays.asList("John Woo", "Jeff Dean", "Josh Bloch", "Josh Long").stream()
				.map(name -> name.split(" "))
				.collect(Collectors.toList());

		// Use a Java 8 stream to print out each tuple of the list
		splitUpNames.forEach(name -> log.info(String.format("Inserting customer record for %s %s", name[0], name[1])));

		// Uses JdbcTemplate's batchUpdate operation to bulk load data
		jdbcTemplate.batchUpdate("INSERT INTO customers(first_name, last_name) VALUES (?,?)", splitUpNames);
		jdbcTemplate.execute("INSERT INTO customers(first_name, last_name) VALUES ('Martin','codenest')");
		jdbcTemplate.update("INSERT INTO customers(first_name, last_name) VALUES (?,?)", "Martin", "codenestNamedParameter");
		namedParameterJdbcTemplate.update("INSERT INTO customers(first_name, last_name) VALUES (:firstName,:lastName)",
				Map.of("firstName", "Martin", "lastName", "codeNestNamedJdbcTemplate"));

		log.info("Querying for customer records where first_name = 'Josh':");
		jdbcTemplate.query(
				"SELECT id, first_name, last_name FROM customers WHERE first_name = ?", new Object[] { "Josh" },
				(rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("first_name"), rs.getString("last_name"))
		).forEach(customer -> log.info(customer.toString()));

		jdbcTemplate.query(
				"SELECT id, first_name, last_name FROM customers WHERE first_name = ?", new Object[] { "Martin" },
				(rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("first_name"), rs.getString("last_name"))
		).forEach(p-> log.info(p.toString()));


		// Queries:
		String sql = "SELECT id, first_name, last_name FROM customers WHERE id = ?";
		Long id = 1L;
		Customer customer = jdbcTemplate.queryForObject(sql, new BeanPropertyRowMapper<>(Customer.class), id);
		Customer customer2 = jdbcTemplate.queryForObject(sql, new CustomerRowMapper(), id);

		Customer hans = new Customer("Hans", "Mueller");
		BeanPropertySqlParameterSource beanPropertySqlParameterSource = new BeanPropertySqlParameterSource(hans);
		beanPropertySqlParameterSource.registerSqlType("yesNoEnumInCustomer", Types.VARCHAR);
		namedParameterJdbcTemplate.update("Insert into Customers (first_name, last_name) Values(:firstName, :lastName)", beanPropertySqlParameterSource);

		jdbcTemplate.query(
				"SELECT id, first_name, last_name FROM customers WHERE first_name = ?", new Object[] { "Hans" },
				(rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("first_name"), rs.getString("last_name"))
		).forEach(p-> log.info(p.toString()));
	}

}
