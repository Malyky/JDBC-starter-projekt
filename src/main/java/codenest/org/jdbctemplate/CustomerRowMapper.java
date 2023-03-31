package codenest.org.jdbctemplate;

import codenest.org.jdbctemplate.data.Customer;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CustomerRowMapper implements RowMapper<Customer> {
    @Override
    public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
        Customer customer = new Customer();
        customer.setId(rs.getInt("id"));
        customer.setFirstName(rs.getString("first_Name"));
        customer.setLastName(rs.getString("last_Name")); // also can use index like rs.getString(3)

        return customer;

    }
}
