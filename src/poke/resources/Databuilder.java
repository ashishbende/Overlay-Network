package poke.resources;

import java.util.HashMap;

public class Databuilder {

	public static void main(String[] args) {
		generateData();
	}

	public static void generateData() {
		DataResource res = new DataResource();
		for (int i = 1; i < 9; i++) {
			res.addCourse("CMPE27" + i, "Course Description-" + i);
			res.addUser("User-" + i);
		}

		for (int i = 1; i < 9; i++) {
			for (int j = 1; j < 5; j++) {
				res.addUserCourse(i, j);
			}
		}
		
		DataResource res1 = new DataResource();
		
		DataResource.Users u = res1.getUserDetails(2);
		
		System.out.println(u);
		
		String result = "";
		String lstUsercourses = "";
		
		if (u != null) {
			lstUsercourses = lstUsercourses + "[";
			for(DataResource.UserCourses uc : u.listUserCourse){
				lstUsercourses = lstUsercourses + "{";
				lstUsercourses = lstUsercourses + "UserId:" + uc.UserId + ", CourseId:" + uc.CourseId;
				lstUsercourses = lstUsercourses + "},";
			}
			
			lstUsercourses = lstUsercourses + "]";
			result = "{UserId: " + u.UserId + ", UserName: " + u.UserName + ", UserCourses: " + lstUsercourses + "}";
		}

		HashMap<Integer, DataResource.Course> a = res1.getAllCourseDetails();

		HashMap<Integer, DataResource.Users> b = res1.getAllUserDetails();

	}

}
