`timescale 1ns / 1ps
//////////////////////////////////////////////////////////////////////////////////
// Company: 
// Engineer: 
// 
// Create Date: 06/22/2026 11:15:16 PM
// Design Name: 
// Module Name: top
// Project Name: 
// Target Devices: 
// Tool Versions: 
// Description: 
// 
// Dependencies: 
// 
// Revision:
// Revision 0.01 - File Created
// Additional Comments:
// 
//////////////////////////////////////////////////////////////////////////////////


module top(
btn
    );
    
    input btn;
    
     wire [1:0] btn;
     wire [1:0] enable;

  zynq_sys zynq_sys_i
       (.enable(enable),
       .enable_snow(enable[1]),
        .enable_white(enable[0]),
        .btns_2bits_tri_i(btn));
endmodule
