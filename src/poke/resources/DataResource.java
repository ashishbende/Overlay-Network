package poke.resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DataResource {

	public static HashMap<Integer, Course> listOfCourses = new HashMap<Integer, Course>();
	public static HashMap<Integer, Users> userDetails = new HashMap<Integer, Users>();
	public static Integer udStartIndex = 1;
	public static Integer lcStartIndex = 1;

	public Users getUserDetails(int userId) {
		return userDetails.get(userId);
	}
	
	public HashMap<Integer, Course> getAllCourseDetails(){
		return this.listOfCourses;
	}
	
	public HashMap<Integer, Users> getAllUserDetails(){
		return this.userDetails;
	}

	public Course getCourseDetails(int courseId) {
		return listOfCourses.get(courseId);
	}

	public boolean addCourse(String courseName, String courseDescription) {
		int key = lcStartIndex;
		listOfCourses.put(key, new Course(key, courseName, courseDescription));
		lcStartIndex++;
		return true;
	}

	public boolean addUser(String username) {
		List<UserCourses> userDetailsList = new ArrayList<UserCourses>();
		int key = udStartIndex;
		userDetails.put(key, new Users(key, username, userDetailsList));
		udStartIndex++;
		return true;
	}

	public boolean addUserCourse(int userId, int courseId) {
		Users details = userDetails.get(userId);
		details.listUserCourse.add(new UserCourses(userId, courseId));
		return true;
	}

	public List<UserCourses> getUserCourses(int userId) {
		Users details = userDetails.get(userId);
		return details.listUserCourse;
	}

	public static class Course {
		Integer CourseId;
		String CourseName;
		String CourseDescription;

		public Course(int courseId, String courseName, String courseDesc) {
			this.CourseId = courseId;
			this.CourseName = courseName;
			this.CourseDescription = courseDesc;
		}
	}

	public static class Users {
		Integer UserId;
		String UserName;
		List<UserCourses> listUserCourse = new ArrayList<UserCourses>();

		public Users(int userId, String userName, List<UserCourses> userCourses) {
			this.UserId = userId;
			this.UserName = userName;
			this.listUserCourse = userCourses;
		}
	}

	public static class UserCourses {
		Integer UserId;
		Integer CourseId;

		public UserCourses(int userId, int courseId) {
			this.UserId = userId;
			this.CourseId = courseId;
		}
	}
}
